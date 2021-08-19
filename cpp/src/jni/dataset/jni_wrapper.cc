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

#include <mutex>

#include "arrow/array.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/ipc/api.h"
#include "arrow/jniutil/jni_util.h"
#include "arrow/util/iterator.h"
#include "jni/dataset/DTypes.pb.h"
#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_NativeMemoryPool.h"

namespace {

jclass illegal_access_exception_class;
jclass illegal_argument_exception_class;
jclass runtime_exception_class;

jclass serialized_record_batch_iterator_class;
jclass java_reservation_listener_class;

jmethodID serialized_record_batch_iterator_hasNext;
jmethodID serialized_record_batch_iterator_next;
jmethodID reserve_memory_method;
jmethodID unreserve_memory_method;

jlong default_memory_pool_id = -1L;

jint JNI_VERSION = JNI_VERSION_1_6;

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : runtime_error(arg) {}
};

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    ThrowPendingException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

void JniAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    ThrowPendingException(status.message());
  }
}

void JniThrow(std::string message) { ThrowPendingException(message); }

arrow::Result<std::shared_ptr<arrow::dataset::FileFormat>> GetFileFormat(
    jint file_format_id) {
  switch (file_format_id) {
    case 0:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    case 1:
      return std::make_shared<arrow::dataset::CsvFileFormat>();
    default:
      std::string error_message =
          "illegal file format id: " + std::to_string(file_format_id);
      return arrow::Status::Invalid(error_message);
  }
}

class ReserveFromJava : public arrow::jniutil::ReservationListener {
 public:
  ReserveFromJava(JavaVM* vm, jobject java_reservation_listener)
      : vm_(vm), java_reservation_listener_(java_reservation_listener) {}

  arrow::Status OnReservation(int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
    env->CallObjectMethod(java_reservation_listener_, reserve_memory_method, size);
    RETURN_NOT_OK(arrow::jniutil::CheckException(env));
    return arrow::Status::OK();
  }

  arrow::Status OnRelease(int64_t size) override {
    JNIEnv* env;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
      return arrow::Status::Invalid("JNIEnv was not attached to current thread");
    }
    env->CallObjectMethod(java_reservation_listener_, unreserve_memory_method, size);
    RETURN_NOT_OK(arrow::jniutil::CheckException(env));
    return arrow::Status::OK();
  }

  jobject GetJavaReservationListener() { return java_reservation_listener_; }

 private:
  JavaVM* vm_;
  jobject java_reservation_listener_;
};

/// \class DisposableScannerAdaptor
/// \brief An adaptor that iterates over a Scanner instance then returns RecordBatches
/// directly.
///
/// This lessens the complexity of the JNI bridge to make sure it to be easier to
/// maintain. On Java-side, NativeScanner can only produces a single NativeScanTask
/// instance during its whole lifecycle. Each task stands for a DisposableScannerAdaptor
/// instance through JNI bridge.
///
class DisposableScannerAdaptor {
 public:
  DisposableScannerAdaptor(std::shared_ptr<arrow::dataset::Scanner> scanner,
                           arrow::dataset::TaggedRecordBatchIterator batch_itr)
      : scanner_(std::move(scanner)), batch_itr_(std::move(batch_itr)) {}

  static arrow::Result<std::shared_ptr<DisposableScannerAdaptor>> Create(
      std::shared_ptr<arrow::dataset::Scanner> scanner) {
    ARROW_ASSIGN_OR_RAISE(auto batch_itr, scanner->ScanBatches())
    return std::make_shared<DisposableScannerAdaptor>(scanner, std::move(batch_itr));
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, NextBatch());
    return batch;
  }

  const std::shared_ptr<arrow::dataset::Scanner>& GetScanner() const { return scanner_; }

 private:
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
  arrow::dataset::TaggedRecordBatchIterator batch_itr_;

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextBatch() {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_itr_.Next())
    return batch.record_batch;
  }
};

bool ParseProtobuf(uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream cis(buf, bufLen);
  cis.SetRecursionLimit(1000);
  return msg->ParseFromCodedStream(&cis);
}

void releaseFilterInput(jbyteArray condition_arr, jbyte* condition_bytes, JNIEnv* env) {
  env->ReleaseByteArrayElements(condition_arr, condition_bytes, JNI_ABORT);
}

// fixme in development. Not all node types considered.
arrow::compute::Expression translateNode(arrow::dataset::types::TreeNode node,
                                         JNIEnv* env) {
  if (node.has_fieldnode()) {
    const arrow::dataset::types::FieldNode& f_node = node.fieldnode();
    const std::string& name = f_node.name();
    return arrow::compute::field_ref(name);
  }
  if (node.has_intnode()) {
    const arrow::dataset::types::IntNode& int_node = node.intnode();
    int32_t val = int_node.value();
    return arrow::compute::literal(val);
  }
  if (node.has_longnode()) {
    const arrow::dataset::types::LongNode& long_node = node.longnode();
    int64_t val = long_node.value();
    return arrow::compute::literal(val);
  }
  if (node.has_floatnode()) {
    const arrow::dataset::types::FloatNode& float_node = node.floatnode();
    float val = float_node.value();
    return arrow::compute::literal(val);
  }
  if (node.has_doublenode()) {
    const arrow::dataset::types::DoubleNode& double_node = node.doublenode();
    double val = double_node.value();
    return arrow::compute::literal(std::make_shared<arrow::DoubleScalar>(val));
  }
  if (node.has_booleannode()) {
    const arrow::dataset::types::BooleanNode& boolean_node = node.booleannode();
    bool val = boolean_node.value();
    return arrow::compute::literal(val);
  }
  if (node.has_andnode()) {
    const arrow::dataset::types::AndNode& and_node = node.andnode();
    const arrow::dataset::types::TreeNode& left_arg = and_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = and_node.rightarg();
    return arrow::compute::and_(translateNode(left_arg, env),
                                translateNode(right_arg, env));
  }
  if (node.has_ornode()) {
    const arrow::dataset::types::OrNode& or_node = node.ornode();
    const arrow::dataset::types::TreeNode& left_arg = or_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = or_node.rightarg();
    return arrow::compute::or_(translateNode(left_arg, env),
                               translateNode(right_arg, env));
  }
  if (node.has_cpnode()) {
    const arrow::dataset::types::ComparisonNode& cp_node = node.cpnode();
    const std::string& op_name = cp_node.opname();
    const arrow::dataset::types::TreeNode& left_arg = cp_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = cp_node.rightarg();
    return arrow::compute::call(
        op_name, {translateNode(left_arg, env), translateNode(right_arg, env)});
  }
  if (node.has_notnode()) {
    const arrow::dataset::types::NotNode& not_node = node.notnode();
    const ::arrow::dataset::types::TreeNode& child = not_node.args();
    arrow::compute::Expression translatedChild = translateNode(child, env);
    return arrow::compute::not_(translatedChild);
  }
  if (node.has_isvalidnode()) {
    const arrow::dataset::types::IsValidNode& is_valid_node = node.isvalidnode();
    const ::arrow::dataset::types::TreeNode& child = is_valid_node.args();
    arrow::compute::Expression translatedChild = translateNode(child, env);
    return arrow::compute::call("is_valid", {translatedChild});
  }
  std::string error_message = "Unknown node type";
  env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  return arrow::compute::literal(false);  // unreachable
}

arrow::compute::Expression translateFilter(arrow::dataset::types::Condition condition,
                                           JNIEnv* env) {
  const arrow::dataset::types::TreeNode& tree_node = condition.root();
  return translateNode(tree_node, env);
}

/// \brief Simple scan task implementation that is constructed directly
/// from a record batch iterator (and its corresponding fragment).
class SimpleIteratorTask : public arrow::dataset::ScanTask {
 public:
  SimpleIteratorTask(std::shared_ptr<arrow::dataset::ScanOptions> options,
                     std::shared_ptr<arrow::dataset::Fragment> fragment,
                     arrow::RecordBatchIterator itr)
      : ScanTask(options, fragment) {
    this->itr_ = std::move(itr);
  }

  static arrow::Result<std::shared_ptr<SimpleIteratorTask>> Make(
      arrow::RecordBatchIterator itr,
      std::shared_ptr<arrow::dataset::ScanOptions> options,
      std::shared_ptr<arrow::dataset::Fragment> fragment) {
    return std::make_shared<SimpleIteratorTask>(options, fragment, std::move(itr));
  }

  arrow::Result<arrow::RecordBatchIterator> Execute() override {
    if (used_) {
      return arrow::Status::Invalid(
          "SimpleIteratorFragment is disposable and"
          "already scanned");
    }
    used_ = true;
    return std::move(itr_);
  }

 private:
  arrow::RecordBatchIterator itr_;
  bool used_ = false;
};

/// \brief Simple fragment implementation that is constructed directly
/// from a record batch iterator.
class SimpleIteratorFragment : public arrow::dataset::Fragment {
 public:
  explicit SimpleIteratorFragment(arrow::RecordBatchIterator itr)
      : arrow::dataset::Fragment() {
    itr_ = std::move(itr);
  }

  static arrow::Result<std::shared_ptr<SimpleIteratorFragment>> Make(
      arrow::RecordBatchIterator itr) {
    return std::make_shared<SimpleIteratorFragment>(std::move(itr));
  }

  arrow::Result<arrow::RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<arrow::dataset::ScanOptions>& options) override {
    return arrow::Status::NotImplemented("Aysnc scan not supported");
  }

  arrow::Result<arrow::dataset::ScanTaskIterator> Scan(
      std::shared_ptr<arrow::dataset::ScanOptions> options) override {
    if (used_) {
      return arrow::Status::Invalid(
          "SimpleIteratorFragment is disposable and"
          "already scanned");
    }
    used_ = true;
    ARROW_ASSIGN_OR_RAISE(
        auto task, SimpleIteratorTask::Make(std::move(itr_), options, shared_from_this()))
    return arrow::MakeVectorIterator<std::shared_ptr<arrow::dataset::ScanTask>>({task});
  }

  std::string type_name() const override { return "simple_iterator"; }

  arrow::Result<std::shared_ptr<arrow::Schema>> ReadPhysicalSchemaImpl() override {
    return arrow::Status::NotImplemented("No physical schema is readable");
  }

 private:
  arrow::RecordBatchIterator itr_;
  bool used_ = false;
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FromBytes(
    JNIEnv* env, std::shared_ptr<arrow::Schema> schema, jbyteArray bytes) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                        arrow::jniutil::DeserializeUnsafeFromJava(env, schema, bytes))
  return batch;
}

/// \brief Create scanner that scans over Java dataset API's components.
///
/// Currently, we use a NativeSerializedRecordBatchIterator as the underlying
/// Java object to do scanning. Which means, only one single task will
/// be produced from C++ code.
arrow::Result<std::shared_ptr<arrow::dataset::Scanner>> MakeJavaDatasetScanner(
    JavaVM* vm, jobject java_serialized_record_batch_iterator,
    std::shared_ptr<arrow::Schema> schema) {
  arrow::RecordBatchIterator itr = arrow::MakeFunctionIterator(
      [vm, java_serialized_record_batch_iterator,
       schema]() -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
        JNIEnv* env;
        if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
          return arrow::Status::Invalid("JNIEnv was not attached to current thread");
        }
        if (!env->CallBooleanMethod(java_serialized_record_batch_iterator,
                                    serialized_record_batch_iterator_hasNext)) {
          return nullptr;  // stream ended
        }
        auto bytes = (jbyteArray)env->CallObjectMethod(
            java_serialized_record_batch_iterator, serialized_record_batch_iterator_next);
        RETURN_NOT_OK(arrow::jniutil::CheckException(env));
        ARROW_ASSIGN_OR_RAISE(auto batch, FromBytes(env, schema, bytes));
        return batch;
      });

  ARROW_ASSIGN_OR_RAISE(auto fragment, SimpleIteratorFragment::Make(std::move(itr)))

  arrow::dataset::ScannerBuilder scanner_builder(
      std::move(schema), fragment, std::make_shared<arrow::dataset::ScanOptions>());
  // Use default memory pool is enough as native allocation is ideally
  // not being called during scanning Java-based fragments.
  RETURN_NOT_OK(scanner_builder.Pool(arrow::default_memory_pool()));
  return scanner_builder.Finish();
}
}  // namespace

using arrow::jniutil::CreateGlobalClassReference;
using arrow::jniutil::CreateNativeRef;
using arrow::jniutil::FromSchemaByteArray;
using arrow::jniutil::GetMethodID;
using arrow::jniutil::JStringToCString;
using arrow::jniutil::ReleaseNativeRef;
using arrow::jniutil::RetrieveNativeInstance;
using arrow::jniutil::ToSchemaByteArray;
using arrow::jniutil::ToStringVector;

using arrow::jniutil::ReservationListenableMemoryPool;
using arrow::jniutil::ReservationListener;

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                 \
  }                                                   \
  catch (JniPendingException & e) {                   \
    env->ThrowNew(runtime_exception_class, e.what()); \
    return fallback_expr;                             \
  }
// macro ended

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  JNI_METHOD_START
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  serialized_record_batch_iterator_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/NativeSerializedRecordBatchIterator;");
  java_reservation_listener_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/ReservationListener;");
  serialized_record_batch_iterator_hasNext = JniGetOrThrow(
      GetMethodID(env, serialized_record_batch_iterator_class, "hasNext", "()Z"));
  serialized_record_batch_iterator_next = JniGetOrThrow(
      GetMethodID(env, serialized_record_batch_iterator_class, "next", "()[B"));
  reserve_memory_method =
      JniGetOrThrow(GetMethodID(env, java_reservation_listener_class, "reserve", "(J)V"));
  unreserve_memory_method = JniGetOrThrow(
      GetMethodID(env, java_reservation_listener_class, "unreserve", "(J)V"));

  default_memory_pool_id = reinterpret_cast<jlong>(arrow::default_memory_pool());

  return JNI_VERSION;
  JNI_METHOD_END(JNI_ERR)
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(serialized_record_batch_iterator_class);
  env->DeleteGlobalRef(java_reservation_listener_class);

  default_memory_pool_id = -1L;
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    getDefaultMemoryPool
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_jni_NativeMemoryPool_getDefaultMemoryPool(JNIEnv* env,
                                                                        jclass) {
  JNI_METHOD_START
  return default_memory_pool_id;
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    createListenableMemoryPool
 * Signature: (Lorg/apache/arrow/memory/ReservationListener;)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_jni_NativeMemoryPool_createListenableMemoryPool(
    JNIEnv* env, jclass, jobject jlistener) {
  JNI_METHOD_START
  jobject jlistener_ref = env->NewGlobalRef(jlistener);
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  std::shared_ptr<ReservationListener> listener =
      std::make_shared<ReserveFromJava>(vm, jlistener_ref);
  auto memory_pool = new ReservationListenableMemoryPool(arrow::default_memory_pool(),
                                                         listener, 8 * 1024 * 1024);
  return reinterpret_cast<jlong>(memory_pool);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    releaseMemoryPool
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_apache_arrow_dataset_jni_NativeMemoryPool_releaseMemoryPool(
    JNIEnv* env, jclass, jlong memory_pool_id) {
  JNI_METHOD_START
  if (memory_pool_id == default_memory_pool_id) {
    return;
  }
  ReservationListenableMemoryPool* pool =
      reinterpret_cast<ReservationListenableMemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    return;
  }
  std::shared_ptr<ReserveFromJava> rm =
      std::dynamic_pointer_cast<ReserveFromJava>(pool->get_listener());
  if (rm == nullptr) {
    delete pool;
    return;
  }
  delete pool;
  env->DeleteGlobalRef(rm->GetJavaReservationListener());
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_NativeMemoryPool
 * Method:    bytesAllocated
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_NativeMemoryPool_bytesAllocated(
    JNIEnv* env, jclass, jlong memory_pool_id) {
  JNI_METHOD_START
  arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool instance not found. It may not exist nor has been closed");
  }
  return pool->bytes_allocated();
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDatasetFactory
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDatasetFactory(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::dataset::DatasetFactory>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    inspectSchema
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_inspectSchema(
    JNIEnv* env, jobject, jlong dataset_factor_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      RetrieveNativeInstance<arrow::dataset::DatasetFactory>(dataset_factor_id);
  std::shared_ptr<arrow::Schema> schema = JniGetOrThrow(d->Inspect());
  return JniGetOrThrow(ToSchemaByteArray(env, schema));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createDataset
 * Signature: (J[B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createDataset(
    JNIEnv* env, jobject, jlong dataset_factory_id, jbyteArray schema_bytes) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      RetrieveNativeInstance<arrow::dataset::DatasetFactory>(dataset_factory_id);
  std::shared_ptr<arrow::Schema> schema;
  schema = JniGetOrThrow(FromSchemaByteArray(env, schema_bytes));
  std::shared_ptr<arrow::dataset::Dataset> dataset = JniGetOrThrow(d->Finish(schema));
  return CreateNativeRef(dataset);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataset
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataset(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::dataset::Dataset>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createScanner
 * Signature: (J[Ljava/lang/String;[BJJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createScanner(
    JNIEnv* env, jobject, jlong dataset_id, jobjectArray columns, jbyteArray filter,
    jlong batch_size, jlong memory_pool_id) {
  JNI_METHOD_START
  arrow::MemoryPool* pool = reinterpret_cast<arrow::MemoryPool*>(memory_pool_id);
  if (pool == nullptr) {
    JniThrow("Memory pool does not exist or has been closed");
  }
  std::shared_ptr<arrow::dataset::Dataset> dataset =
      RetrieveNativeInstance<arrow::dataset::Dataset>(dataset_id);
  std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder =
      JniGetOrThrow(dataset->NewScan());
  JniAssertOkOrThrow(scanner_builder->Pool(pool));

  std::vector<std::string> column_vector = ToStringVector(env, columns);
  if (!column_vector.empty()) {
    JniAssertOkOrThrow(scanner_builder->Project(column_vector));
  }
  JniAssertOkOrThrow(scanner_builder->BatchSize(batch_size));
  // initialize filters
  jsize exprs_len = env->GetArrayLength(filter);
  jbyte* exprs_bytes = env->GetByteArrayElements(filter, 0);
  arrow::dataset::types::Condition condition;
  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(exprs_bytes), exprs_len, &condition)) {
    releaseFilterInput(filter, exprs_bytes, env);
    std::string error_message = "bad protobuf message";
    env->ThrowNew(illegal_argument_exception_class, error_message.c_str());
  }
  if (condition.has_root()) {
    JniAssertOkOrThrow(scanner_builder->Filter(translateFilter(condition, env)));
  }

  auto scanner = JniGetOrThrow(scanner_builder->Finish());
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      JniGetOrThrow(DisposableScannerAdaptor::Create(scanner));
  jlong id = CreateNativeRef(scanner_adaptor);
  return id;
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanner
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanner(
    JNIEnv* env, jobject, jlong scanner_id) {
  JNI_METHOD_START
  ReleaseNativeRef<DisposableScannerAdaptor>(scanner_id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getSchemaFromScanner
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_dataset_jni_JniWrapper_getSchemaFromScanner(JNIEnv* env, jobject,
                                                                  jlong scanner_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema =
      RetrieveNativeInstance<DisposableScannerAdaptor>(scanner_id)
          ->GetScanner()
          ->options()
          ->projected_schema;
  return JniGetOrThrow(ToSchemaByteArray(env, schema));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    nextRecordBatch
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_nextRecordBatch(
    JNIEnv* env, jobject, jlong scanner_id) {
  JNI_METHOD_START
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      RetrieveNativeInstance<DisposableScannerAdaptor>(scanner_id);

  std::shared_ptr<arrow::RecordBatch> record_batch =
      JniGetOrThrow(scanner_adaptor->Next());
  if (record_batch == nullptr) {
    return nullptr;  // stream ended
  }
  return JniGetOrThrow(arrow::jniutil::SerializeUnsafeFromNative(env, record_batch));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    releaseBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_releaseBuffer(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::Buffer>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    newJniGlobalReference
 * Signature: (Ljava/lang/Object;)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_newJniGlobalReference(JNIEnv* env, jobject,
                                                                    jobject referent) {
  JNI_METHOD_START
  return reinterpret_cast<jlong>(env->NewGlobalRef(referent));
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    newJniMethodReference
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_newJniMethodReference(JNIEnv* env, jobject,
                                                                    jstring class_sig,
                                                                    jstring method_name,
                                                                    jstring method_sig) {
  JNI_METHOD_START
  jclass clazz = env->FindClass(JStringToCString(env, class_sig).data());
  jmethodID jmethod_id =
      env->GetMethodID(clazz, JStringToCString(env, method_name).data(),
                       JStringToCString(env, method_sig).data());
  return reinterpret_cast<jlong>(jmethod_id);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeFileSystemDatasetFactory
 * Signature: (Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_makeFileSystemDatasetFactory(
    JNIEnv* env, jobject, jstring uri, jint file_format_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  arrow::dataset::FileSystemFactoryOptions options;
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      JniGetOrThrow(arrow::dataset::FileSystemDatasetFactory::Make(
          JStringToCString(env, uri), file_format, options));
  return CreateNativeRef(d);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    writeFromScannerToFile
 * Signature:
 * (Lorg/apache/arrow/dataset/jni/NativeSerializedRecordBatchIterator;[BILjava/lang/String;[Ljava/lang/String;ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_writeFromScannerToFile(
    JNIEnv* env, jobject, jobject itr, jbyteArray schema_bytes, jint file_format_id,
    jstring uri, jobjectArray partition_columns, jint max_partitions,
    jstring base_name_template) {
  JNI_METHOD_START
  JavaVM* vm;
  if (env->GetJavaVM(&vm) != JNI_OK) {
    JniThrow("Unable to get JavaVM instance");
  }
  auto schema = JniGetOrThrow(FromSchemaByteArray(env, schema_bytes));
  auto scanner = JniGetOrThrow(MakeJavaDatasetScanner(vm, itr, schema));
  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  arrow::dataset::FileSystemDatasetWriteOptions options;
  std::string output_path;
  auto filesystem = JniGetOrThrow(
      arrow::fs::FileSystemFromUri(JStringToCString(env, uri), &output_path));
  std::vector<std::string> partition_column_vector =
      ToStringVector(env, partition_columns);
  options.file_write_options = file_format->DefaultWriteOptions();
  options.filesystem = filesystem;
  options.base_dir = output_path;
  options.basename_template = JStringToCString(env, base_name_template);
  options.partitioning = std::make_shared<arrow::dataset::HivePartitioning>(
      arrow::dataset::SchemaFromColumnNames(schema, partition_column_vector));
  options.max_partitions = max_partitions;
  JniAssertOkOrThrow(arrow::dataset::FileSystemDataset::Write(options, scanner));
  JNI_METHOD_END()
}
