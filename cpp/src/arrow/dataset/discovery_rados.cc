
#include "arrow/dataset/discovery_rados.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/path_forest.h"
#include "arrow/dataset/discovery.h"

namespace arrow {
namespace dataset {

arrow::Result<std::shared_ptr<DatasetFactory>> arrow::dataset::RadosDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, const std::vector<std::string>& paths,
    std::shared_ptr<FileFormat> format,
    arrow::dataset::FileSystemFactoryOptions options,
    const std::string& path_to_config) {
  const auto fsdf = FileSystemDatasetFactory::Make(std::move(filesystem), paths, std::move(format), std::move(options));
  return std::shared_ptr<DatasetFactory>(new RadosDatasetFactory(fsdf, path_to_config));
}


Result<std::shared_ptr<DatasetFactory>> RadosDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, fs::FileSelector selector,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options,
    const std::string& path_to_config) {
  const auto fsdf = FileSystemDatasetFactory::Make(std::move(filesystem), std::move(selector), std::move(format), std::move(options));
  return std::shared_ptr<DatasetFactory>(new RadosDatasetFactory(fsdf, path_to_config));
}


Result<std::shared_ptr<DatasetFactory>> RadosDatasetFactory::Make(
    std::string uri, int64_t start_offset, int64_t length,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options,
    const std::string& path_to_config) {
  const auto fsdf = FileSystemDatasetFactory::Make(std::move(uri), start_offset, length, std::move(format), std::move(options));
  return std::shared_ptr<DatasetFactory>(new RadosDatasetFactory(fsdf, path_to_config));
}


Result<std::shared_ptr<Dataset>> RadosDatasetFactory::Finish(FinishOptions options) {
  std::shared_ptr<Schema> schema = options.schema;
  bool schema_missing = schema == nullptr;
  if (schema_missing) {
    ARROW_ASSIGN_OR_RAISE(schema, Inspect(options.inspect_options));
  }

  if (options.validate_fragments && !schema_missing) {
    // If the schema was not explicitly provided we don't need to validate
    // since Inspect has already succeeded in producing a valid unified schema.
    ARROW_ASSIGN_OR_RAISE(auto schemas, InspectSchemas(options.inspect_options));
    for (const auto& s : schemas) {
      RETURN_NOT_OK(SchemaBuilder::AreCompatible({schema, s}));
    }
  }

  std::shared_ptr<Partitioning> partitioning = options_.partitioning.partitioning();
  if (partitioning == nullptr) {
    auto factory = options_.partitioning.factory();
    ARROW_ASSIGN_OR_RAISE(partitioning, factory->Finish(schema));
  }

  std::vector<std::shared_ptr<FileFragment>> fragments;
  for (const auto& info : files_) {
    auto fixed_path = StripPrefixAndFilename(info.path(), options_.partition_base_dir);
    ARROW_ASSIGN_OR_RAISE(auto partition, partitioning->Parse(fixed_path));
    ARROW_ASSIGN_OR_RAISE(auto fragment, format_->MakeFragment(info, partition, 1, schema));
    fragments.push_back(fragment);
  }

  return FileSystemDataset::Make(schema, root_partition_, format_, fs_, fragments);
}

Result<std::shared_ptr<DatasetFactory>> RadosDatasetFactory::Make(
    std::shared_ptr<fs::FileSystem> filesystem, const std::vector<fs::FileInfo>& files,
    std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options,
    const std::string& path_to_config) {
  const auto fsdf = FileSystemDatasetFactory::Make(std::move(filesystem), files, std::move(format), std::move(options));
  std::shared_ptr<DatasetFactory> x = *fsdf;
  return std::shared_ptr<DatasetFactory>(new RadosDatasetFactory(fsdf, path_to_config));
}

RadosDatasetFactory::RadosDatasetFactory(Result<std::shared_ptr<DatasetFactory>> fsDatasetFactory, const std::string& path_to_config)
    : FileSystemDatasetFactory(*reinterpret_cast<FileSystemDatasetFactory*>((*fsDatasetFactory).get())), delegate(*fsDatasetFactory), path_to_config(path_to_config) {}

}  // namespace dataset
}  // namespace arrow