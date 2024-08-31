// Copyright (c) 2024-present, OpenAtom Foundation, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory

/*
  Responsible for interfacing with RocksDB to effectively
  manage kiwi's data.
 */

#include "db.h"
#include <algorithm>

#include "config.h"
#include "praft/praft.h"
#include "pstd/log.h"

extern kiwi::PConfig g_config;

namespace kiwi {

DB::DB(int db_index, const std::string& db_path)
    : db_index_(db_index), db_path_(db_path + std::to_string(db_index_) + '/') {}

DB::~DB() { INFO("DB{} is closing...", db_index_); }

rocksdb::Status DB::Open() {
  storage::StorageOptions storage_options;
  storage_options.options = g_config.GetRocksDBOptions();
  storage_options.table_options = g_config.GetRocksDBBlockBasedTableOptions();

  storage_options.options.ttl = g_config.rocksdb_ttl_second.load(std::memory_order_relaxed);
  storage_options.options.periodic_compaction_seconds =
      g_config.rocksdb_periodic_second.load(std::memory_order_relaxed);

  storage_options.small_compaction_threshold = g_config.small_compaction_threshold.load();
  storage_options.small_compaction_duration_threshold = g_config.small_compaction_duration_threshold.load();

  if (g_config.use_raft.load(std::memory_order_relaxed)) {
    storage_options.append_log_function = [&r = PRAFT](const Binlog& log, std::promise<rocksdb::Status>&& promise) {
      r.AppendLog(log, std::move(promise));
    };
    storage_options.do_snapshot_function = [raft = &kiwi::PRAFT](auto&& self_snapshot_index, auto&& is_sync) {
      raft->DoSnapshot(std::forward<decltype(self_snapshot_index)>(self_snapshot_index),
                       std::forward<decltype(is_sync)>(is_sync));
    };
  }

  storage_options.db_instance_num = g_config.db_instance_num.load();
  storage_options.db_id = db_index_;

  std::unique_ptr<storage::Storage> old_storage = std::move(storage_);
  if (old_storage != nullptr) {
    old_storage->Close();
    old_storage.reset();
  }
  storage_ = std::make_unique<storage::Storage>();

  if (auto s = storage_->Open(storage_options, db_path_); !s.ok()) {
    ERROR("Storage open failed! {}", s.ToString());
    abort();
  }

  opened_ = true;
  INFO("Open DB{} success!", db_index_);

  // Cache should not influence the project running states, so cache init code is put after varibale opened_ assignment.
  cache_ = std::make_unique<PCache>(g_config.zset_cache_start_direction.load(),
                                    g_config.zset_cache_field_num_per_key.load());
  // Create cache
  cache::CacheConfig cache_cfg;
  CacheConfigInit(cache_cfg);
  cache_->Init(g_config.cache_num.load(), &cache_cfg);
  return rocksdb::Status::OK();
}

void DB::CacheConfigInit(cache::CacheConfig& cache_cfg) {
  cache_cfg.maxmemory = g_config.cache_maxmemory.load();
  cache_cfg.maxmemory_policy = g_config.cache_maxmemory_policy.load();
  cache_cfg.maxmemory_samples = g_config.cache_maxmemory_samples.load();
  cache_cfg.lfu_decay_time = g_config.cache_lfu_decay_time.load();
}

void DB::CreateCheckpoint(const std::string& checkpoint_path, bool sync) {
  auto checkpoint_sub_path = checkpoint_path + '/' + std::to_string(db_index_);
  if (0 != pstd::CreatePath(checkpoint_sub_path)) {
    WARN("Create dir {} fail !", checkpoint_sub_path);
    return;
  }

  std::shared_lock sharedLock(storage_mutex_);
  auto result = storage_->CreateCheckpoint(checkpoint_sub_path);
  if (sync) {
    for (auto& r : result) {
      r.get();
    }
  }
}

void DB::LoadDBFromCheckpoint(const std::string& checkpoint_path, bool sync [[maybe_unused]]) {
  auto checkpoint_sub_path = checkpoint_path + '/' + std::to_string(db_index_);
  if (0 != pstd::IsDir(checkpoint_sub_path)) {
    WARN("Checkpoint dir {} does not exist!", checkpoint_sub_path);
    return;
  }
  if (0 != pstd::IsDir(db_path_)) {
    if (0 != pstd::CreateDir(db_path_)) {
      WARN("Create dir {} fail !", db_path_);
      return;
    }
  }

  std::lock_guard<std::shared_mutex> lock(storage_mutex_);
  opened_ = false;
  // close the old storage, then open the new storage
  std::unique_ptr<storage::Storage> old_storage = std::move(storage_);
  if (old_storage != nullptr) {
    old_storage->Close();
    old_storage.reset();
  }
  storage_ = std::make_unique<storage::Storage>();
  auto result = storage_->LoadCheckpoint(checkpoint_sub_path, db_path_);

  for (auto& r : result) {
    r.get();
  }

  storage::StorageOptions storage_options;
  storage_options.options = g_config.GetRocksDBOptions();
  storage_options.db_instance_num = g_config.db_instance_num.load();
  storage_options.db_id = db_index_;

  // options for CF
  storage_options.options.ttl = g_config.rocksdb_ttl_second.load(std::memory_order_relaxed);
  storage_options.options.periodic_compaction_seconds =
      g_config.rocksdb_periodic_second.load(std::memory_order_relaxed);
  if (g_config.use_raft.load(std::memory_order_relaxed)) {
    storage_options.append_log_function = [&r = PRAFT](const Binlog& log, std::promise<rocksdb::Status>&& promise) {
      r.AppendLog(log, std::move(promise));
    };
    storage_options.do_snapshot_function =
        std::bind(&kiwi::PRaft::DoSnapshot, &kiwi::PRAFT, std::placeholders::_1, std::placeholders::_2);
  }

  if (auto s = storage_->Open(storage_options, db_path_); !s.ok()) {
    ERROR("Storage open failed! {}", s.ToString());
    abort();
  }

  // in single-mode, kiwi will enable wal
  if (!g_config.use_raft.load(std::memory_order_relaxed)) {
    storage_->DisableWal(false);
  }

  opened_ = true;
  INFO("DB{} load a checkpoint from {} success!", db_index_, checkpoint_path);
}
}  // namespace kiwi
