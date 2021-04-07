// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "arrow/dataset/file_rados_parquet.h"

#include "arrow/api.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/expression.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

namespace arrow {
namespace dataset {

class RadosParquetScanTask : public ScanTask {
 public:
  RadosParquetScanTask(std::shared_ptr<ScanOptions> options,
                       std::shared_ptr<ScanContext> context,
                       FileFragment* file,
                       std::shared_ptr<DirectObjectAccess> doa,
                       bool bypass_fap_scantask = true)
      : ScanTask(std::move(options), std::move(context)),
        bypass_fap_scantask(bypass_fap_scantask),
        partition_expression(file->partition_expression()),
        dataset_schema(file->dataset_schema()),
        source_(file->source()),
        doa_(std::move(doa)){}

  Result<RecordBatchIterator> Execute() override {
    ceph::bufferlist* in = new ceph::bufferlist();
    ceph::bufferlist* out = new ceph::bufferlist();

    Status s;
    struct stat st;
    s = doa_->Stat(source_.path(), st);
    if (!s.ok()) {
        return Status::Invalid(s.message());
    }

    ARROW_RETURN_NOT_OK(SerializeScanRequestToBufferlist(options_->filter, partition_expression, options_->projector.schema(), dataset_schema, st.st_size, *in));

    s = doa_->Exec(st.st_ino, "scan_op", *in, *out);
    if (!s.ok()) {
        return Status::ExecutionError(s.message());
    }

    RecordBatchVector batches;
    auto buffer = std::make_shared<Buffer>((uint8_t*)out->c_str(), out->length());
    auto buffer_reader = std::make_shared<io::BufferReader>(buffer);
    ARROW_ASSIGN_OR_RAISE(auto rb_reader,
                          arrow::ipc::RecordBatchStreamReader::Open(buffer_reader));
    return IteratorFromReader(rb_reader);
  }

  const bool bypass_fap_scantask;

 protected:
  Expression partition_expression;
  std::shared_ptr<Schema> dataset_schema;
  FileSource source_;
  std::shared_ptr<DirectObjectAccess> doa_;
};

Result<std::shared_ptr<RadosParquetFileFormat>> RadosParquetFileFormat::Make(
    const std::string& path_to_config) {
  auto cluster = std::make_shared<RadosCluster>(path_to_config);
  RETURN_NOT_OK(cluster->Connect());
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>(cluster);
  return std::make_shared<RadosParquetFileFormat>(doa);
}

RadosParquetFileFormat::RadosParquetFileFormat(const std::string& path_to_config) {
  auto cluster = std::make_shared<RadosCluster>(path_to_config);
  ARROW_CHECK_OK(cluster->Connect());
  auto doa = std::make_shared<arrow::dataset::DirectObjectAccess>(cluster);
  doa_ = doa;
}

Result<std::shared_ptr<Schema>> RadosParquetFileFormat::Inspect(
    const FileSource& source) const {
//  std::shared_ptr<librados::bufferlist> in = std::make_shared<librados::bufferlist>();
//  std::shared_ptr<librados::bufferlist> out = std::make_shared<librados::bufferlist>();
//
//  Status s = doa_->Exec(source.path(), "read_schema", in, out);
//  if (!s.ok()) return Status::ExecutionError(s.message());
//
//  std::vector<std::shared_ptr<Schema>> schemas;
//  ipc::DictionaryMemo empty_memo;
//  io::BufferReader schema_reader((uint8_t*)out->c_str(), out->length());
//  ARROW_ASSIGN_OR_RAISE(auto schema, ipc::ReadSchema(&schema_reader, &empty_memo));
//  return schema;
  ARROW_ASSIGN_OR_RAISE(auto reader, GetReader(source));
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));
  return schema;
}

Result<ScanTaskIterator> RadosParquetFileFormat::ScanFile(
    std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context,
    FileFragment* file) const {
  ScanTaskVector v{std::make_shared<RadosParquetScanTask>(
      std::move(options), std::move(context), file, std::move(doa_))};
  return MakeVectorIterator(v);
}

}  // namespace dataset
}  // namespace arrow
