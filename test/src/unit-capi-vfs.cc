/**
 * @file   unit-capi-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the C API VFS object.
 */

#include "catch.hpp"
#include "tiledb.h"
#include "utils.h"
#ifdef _WIN32
#include "win_filesystem.h"
#else
#include "posix_filesystem.h"
#endif

#include <iostream>
#include <thread>

struct VFSFx {
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_bucket_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
#ifdef _WIN32
  const std::string FILE_TEMP_DIR =
      tiledb::win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_TEMP_DIR =
      std::string("file://") + tiledb::posix::current_dir() + "/tiledb_test/";
#endif

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  VFSFx();
  ~VFSFx();
  void check_vfs(const std::string& path);
  void check_write(const std::string& path);
  void check_move(const std::string& path);
  void check_read(const std::string& path);
  void check_append(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
  void set_supported_fs();
};

VFSFx::VFSFx() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_create(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  if (supports_s3_) {
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.endpoint_override", "localhost:9999", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
  }
  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_vfs_create(ctx_, &vfs_, config);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(tiledb_config_free(config) == TILEDB_OK);
}

VFSFx::~VFSFx() {
  CHECK(tiledb_vfs_free(ctx_, vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
}

void VFSFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_create(&ctx, nullptr) == TILEDB_OK);
  tiledb_vfs_t* vfs;
  REQUIRE(tiledb_vfs_create(ctx, &vfs, nullptr) == TILEDB_OK);

  int supports = 0;
  int rc = tiledb_vfs_supports_fs(ctx, vfs, TILEDB_S3, &supports);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)supports;
  rc = tiledb_vfs_supports_fs(ctx, vfs, TILEDB_HDFS, &supports);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)supports;

  REQUIRE(tiledb_ctx_free(ctx) == TILEDB_OK);
  REQUIRE(tiledb_vfs_free(ctx, vfs) == TILEDB_OK);
}

void VFSFx::check_vfs(const std::string& path) {
  if (supports_s3_) {
    // Check S3 bucket functionality
    if (path == S3_TEMP_DIR) {
      int is_bucket = 0;
      int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
      REQUIRE(rc == TILEDB_OK);
      if (is_bucket) {
        rc = tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str());
        REQUIRE(rc == TILEDB_OK);
      }
      rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
      REQUIRE(rc == TILEDB_OK);
      REQUIRE(!is_bucket);

      rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
      REQUIRE(is_bucket);
    }
  }

  // Create directory, is directory, remove directory
  int is_dir = 0;
  int rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (is_dir) {
    rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_ERR);  // Second time should fail

  // Remove directory recursively
  auto subdir = path + "subdir/";
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);

  // Move
  rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  auto subdir2 = path + "subdir2/";
  rc = tiledb_vfs_move(ctx_, vfs_, subdir.c_str(), subdir2.c_str(), true);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);

  // Invalid file
  int is_file = 0;
  std::string foo_file = path + "foo";
  rc = tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  tiledb_vfs_fh_t* fh;
  rc = tiledb_vfs_open(ctx_, vfs_, foo_file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(fh == nullptr);

  // Touch file
  rc = tiledb_vfs_touch(ctx_, vfs_, foo_file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_remove_file(ctx_, vfs_, foo_file.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Check write and append
  check_write(path);
  check_append(path);

  // Read file
  check_read(path);

  // Move
  check_move(path);

  // Check if filesystem is supported
  int supports = 0;
  rc = tiledb_vfs_supports_fs(ctx_, vfs_, TILEDB_HDFS, &supports);
  CHECK(rc == TILEDB_OK);
  if (supports_hdfs_)
    CHECK(supports);
  else
    CHECK(!supports);
  rc = tiledb_vfs_supports_fs(ctx_, vfs_, TILEDB_S3, &supports);
  CHECK(rc == TILEDB_OK);
  if (supports_s3_)
    CHECK(supports);
  else
    CHECK(!supports);

  if (supports_s3_ && path == S3_TEMP_DIR) {
    int is_empty;
    rc = tiledb_vfs_is_empty_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_empty);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(!(bool)is_empty);
  }

  if (!supports_s3_) {
    rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  if (supports_s3_ && path == S3_TEMP_DIR) {
    rc = tiledb_vfs_empty_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);

    int is_empty;
    rc = tiledb_vfs_is_empty_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_empty);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE((bool)is_empty);

    rc = tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
}

void VFSFx::check_move(const std::string& path) {
  // Move and remove file
  auto file = path + "file";
  auto file2 = path + "file2";
  int is_file = 0;
  int rc = tiledb_vfs_touch(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_move(ctx_, vfs_, file.c_str(), file2.c_str(), true);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);

  // Move directory with subdirectories and files
  auto dir = path + "dir/";
  auto dir2 = path + "dir2/";
  auto subdir = path + "dir/subdir/";
  auto subdir2 = path + "dir2/subdir/";
  file = dir + "file";
  file2 = subdir + "file2";
  auto new_file = dir2 + "file";
  auto new_file2 = subdir2 + "file2";
  int is_dir = 0;
  rc = tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_touch(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_touch(ctx_, vfs_, file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_move(ctx_, vfs_, dir.c_str(), dir2.c_str(), true);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);

  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_is_file(ctx_, vfs_, new_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, new_file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);

  // Move from one bucket to another (only for S3)
  if (supports_s3_) {
    if (path == S3_TEMP_DIR) {
      std::string bucket2 = S3_PREFIX + random_bucket_name("tiledb") + "/";
      std::string subdir3 = bucket2 + "tiledb_test/subdir3/";
      std::string file3 = subdir3 + "file2";
      int is_bucket = 0;

      rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket2.c_str(), &is_bucket);
      REQUIRE(rc == TILEDB_OK);
      if (is_bucket) {
        rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str());
        REQUIRE(rc == TILEDB_OK);
      }

      rc = tiledb_vfs_create_bucket(ctx_, vfs_, bucket2.c_str());
      REQUIRE(rc == TILEDB_OK);

      rc = tiledb_vfs_move(ctx_, vfs_, subdir2.c_str(), subdir3.c_str(), true);
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file);
      REQUIRE(rc == TILEDB_OK);
      REQUIRE(is_file);

      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }
}

void VFSFx::check_write(const std::string& path) {
  // File write and file size
  int is_file = 0;
  auto file = path + "file";
  int rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  if (is_file) {
    rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  std::string to_write = "This will be written to the file";
  tiledb_vfs_fh_t* fh;
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  int is_closed = 0;
  rc = tiledb_vfs_fh_closed(ctx_, fh, &is_closed);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_closed == 0);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_sync(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);

  // Only for S3, sync still does not create the file
  uint64_t file_size = 0;
  if (path.find("s3://") == 0) {
    REQUIRE(!is_file);
  } else {
    REQUIRE(is_file);
    rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(file_size == to_write.size());
  }

  // Close file
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_closed(ctx_, fh, &is_closed);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_closed == 1);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);  // It is a file even for S3
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());

  // Check correctness with read
  std::string to_read;
  to_read.resize(to_write.size());
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_read(ctx_, fh, 0, &to_read[0], file_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_write));
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);

  // Open in WRITE mode again - previous file will be removed
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());  // Not 2*to_write.size()

  // Opening and closing the file without writing, first deletes previous
  // file, but then does not create file as it has no contents
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
}

void VFSFx::check_append(const std::string& path) {
  // File write and file size
  auto file = path + "file";
  tiledb_vfs_fh_t* fh;

  // First write
  std::string to_write = "This will be written to the file";
  int rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);

  // Second write - append
  std::string to_write_2 = "This will be appended to the end of the file";
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_APPEND, &fh);
  if (path.find("s3://") == 0) {  // S3 does not support append
    REQUIRE(rc == TILEDB_ERR);
    REQUIRE(fh == nullptr);
  } else {
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_write(ctx_, fh, to_write_2.c_str(), to_write_2.size());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_close(ctx_, fh);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_fh_free(ctx_, fh);
    REQUIRE(rc == TILEDB_OK);
    uint64_t file_size = 0;
    rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
    REQUIRE(rc == TILEDB_OK);
    uint64_t total_size = to_write.size() + to_write_2.size();
    CHECK(file_size == total_size);

    // Check correctness with read
    std::string to_read;
    to_read.resize(total_size);
    rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_read(ctx_, fh, 0, &to_read[0], total_size);
    REQUIRE(rc == TILEDB_OK);
    CHECK_THAT(to_read, Catch::Equals(to_write + to_write_2));
    rc = tiledb_vfs_close(ctx_, fh);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_fh_free(ctx_, fh);
    REQUIRE(rc == TILEDB_OK);
  }

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

void VFSFx::check_read(const std::string& path) {
  auto file = path + "file";
  std::string to_write = "This will be written to the file";
  tiledb_vfs_fh_t* fh;
  int rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);

  // Read only the "will be written" portion of the file
  std::string to_check = "will be written";
  std::string to_read;
  to_read.resize(to_check.size());
  uint64_t offset = 5;
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_read(ctx_, fh, offset, &to_read[0], to_check.size());
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_check));
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

std::string VFSFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(VFSFx, "C API: Test virtual filesystem", "[capi], [vfs]") {
  check_vfs(FILE_TEMP_DIR);

  if (supports_s3_)
    check_vfs(S3_TEMP_DIR);

  if (supports_hdfs_)
    check_vfs(HDFS_TEMP_DIR);
}

TEST_CASE_METHOD(
    VFSFx,
    "C API: Test virtual filesystem when S3 is not supported",
    "[capi], [vfs]") {
  if (!supports_s3_) {
    tiledb_vfs_t* vfs;
    int rc = tiledb_vfs_create(ctx_, &vfs, nullptr);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_create_bucket(ctx_, vfs, "s3://foo");
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_vfs_free(ctx_, vfs);
    CHECK(rc == TILEDB_OK);
  }
}
