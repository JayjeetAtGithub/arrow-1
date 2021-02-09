//
// Created by radon on 09-02-21.
//

#pragma once

#include "arrow/dataset/discovery.h"

#include "arrow/dataset/partition.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_forest.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"
#include "arrow/util/variant.h"


namespace arrow {
namespace dataset {

/// \brief FileSystemDatasetFactory creates a Dataset from a vector of
/// fs::FileInfo or a fs::FileSelector.
class ARROW_DS_EXPORT RadosDatasetFactory : public FileSystemDatasetFactory {
 public:
  /// \brief Build a FileSystemDatasetFactory from an explicit list of
  /// paths.
  ///
  /// \param[in] filesystem passed to FileSystemDataset
  /// \param[in] paths passed to FileSystemDataset
  /// \param[in] format passed to FileSystemDataset
  /// \param[in] options see FileSystemFactoryOptions for more information.
  /// \param[in] path_to_config Rados configfile to use
  static Result<std::shared_ptr<DatasetFactory>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, const std::vector<std::string>& paths,
      std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options,
      const std::string& path_to_config);

  /// \brief Build a FileSystemDatasetFactory from a fs::FileSelector.
  ///
  /// The selector will expand to a vector of FileInfo. The expansion/crawling
  /// is performed in this function call. Thus, the finalized Dataset is
  /// working with a snapshot of the filesystem.
  //
  /// If options.partition_base_dir is not provided, it will be overwritten
  /// with selector.base_dir.
  ///
  /// \param[in] filesystem passed to FileSystemDataset
  /// \param[in] selector used to crawl and search files
  /// \param[in] format passed to FileSystemDataset
  /// \param[in] options see FileSystemFactoryOptions for more information.
  /// \param[in] path_to_config Rados configfile to use
  static Result<std::shared_ptr<DatasetFactory>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, fs::FileSelector selector,
      std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options,
      const std::string& path_to_config);

  /// \brief Build a FileSystemDatasetFactory from an uri including filesystem
  /// information.
  ///
  /// \param[in] uri passed to FileSystemDataset
  /// \param[in] format passed to FileSystemDataset
  /// \param[in] options see FileSystemFactoryOptions for more information.
  /// \param[in] path_to_config Rados configfile to use
  static Result<std::shared_ptr<DatasetFactory>> Make(std::string uri,
                                                      int64_t start_offset,
                                                      int64_t length,
                                                      std::shared_ptr<FileFormat> format,
                                                      FileSystemFactoryOptions options,
                                                      const std::string& path_to_config);

  Result<std::shared_ptr<Dataset>> Finish(FinishOptions options) override;


 protected:
  static Result<std::shared_ptr<DatasetFactory>> Make(
      std::shared_ptr<fs::FileSystem> filesystem, const std::vector<fs::FileInfo>& files,
      std::shared_ptr<FileFormat> format, FileSystemFactoryOptions options,
      const std::string& path_to_config);

  RadosDatasetFactory(Result<std::shared_ptr<DatasetFactory>> fsDatasetFactory, const std::string& path_to_config);

  std::shared_ptr<DatasetFactory> delegate;
  const std::string& path_to_config;

};

}  // namespace dataset
}  // namespace arrow