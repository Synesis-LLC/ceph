// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OS_BLUESTORE_KERNELDEVICE_H
#define CEPH_OS_BLUESTORE_KERNELDEVICE_H

#include <atomic>

#include "os/fs/FS.h"
#include "include/interval_set.h"

#include "aio.h"
#include "BlockDevice.h"

class KernelDevice : public BlockDevice {
  int fd_direct, fd_buffered;
  uint64_t size;
  uint64_t block_size;
  std::string path;
  FS *fs;
  bool aio, dio;

  Mutex debug_lock;
  interval_set<uint64_t> debug_inflight;

  std::atomic<bool> io_since_flush = {false};
  std::mutex flush_mutex;

  aio_queue_t aio_queue;
  aio_callback_t aio_callback;
  void *aio_callback_priv;
  bool aio_stop;

  struct AioCompletionThread : public Thread {
    KernelDevice *bdev;
    explicit AioCompletionThread(KernelDevice *b) : bdev(b) {}
    void *entry() override {
      bdev->_aio_thread();
      return NULL;
    }
  } aio_thread;

  std::atomic_int injecting_crash;

  void _aio_thread();
  int _aio_start();
  void _aio_stop();

  void _aio_log_start(IOContext *ioc, uint64_t offset, uint64_t length);
  void _aio_log_finish(IOContext *ioc, uint64_t offset, uint64_t length);

  int _sync_write(uint64_t off, bufferlist& bl, bool buffered);

  int _lock();

  int direct_read_unaligned(uint64_t off, uint64_t len, char *buf);

  // stalled aio debugging
  aio_list_t debug_queue;
  std::mutex debug_queue_lock;
  aio_t *debug_oldest = nullptr;
  utime_t debug_stall_since;
  void debug_aio_link(aio_t& aio);
  void debug_aio_unlink(aio_t& aio);

  bool healthy = true;

  double aio_op_timeout = 0.0;
  double aio_op_suicide_timeout = 0.0;
  int aio_reap_max = 0;
  int aio_queue_max_iodepth = 0;

  mutable std::mutex aio_queue_metrics_mutex;
  mutable struct aio_queue_mestrics_t {
    int64_t length_max = 0;
    int64_t length_sum = 0;
    size_t length_count = 0;
    int64_t last_completed_max_us = 0;
    int64_t last_completed_sum_us = 0;
    size_t last_completed_count = 0;
  } aio_queue_mestrics;
  void consume_aio_queue_state(const aio_queue_t::aio_queue_state_t& state);

  struct aio_queue_stats_t : public BlockDevice::stats_t {
    int64_t length_max = 0;
    double length_mean = 0.0;
    int64_t last_completed_max_us = 0;
    double last_completed_mean_us = 0.0;
    aio_queue_t::ops_clock_t::time_point timestamp;
    std::chrono::duration<double> period;
    void dump(Formatter *f) const override;
  };
  double aio_stats_min_period_s = 5.0;
  mutable std::mutex aio_queue_stats_mutex;
  mutable aio_queue_t::ops_clock_t::time_point aio_stats_last_get_timestamp =
      aio_queue_t::ops_clock_t::now();
  mutable aio_queue_stats_t aio_queue_stats;

public:
  KernelDevice(CephContext* cct, aio_callback_t cb, void *cbpriv);

  bool is_healthy() const override { return healthy; }
  std::shared_ptr<BlockDevice::stats_t> get_stats() const override;

  void aio_submit(IOContext *ioc) override;

  uint64_t get_size() const override {
    return size;
  }
  uint64_t get_block_size() const override {
    return block_size;
  }

  int collect_metadata(std::string prefix, map<std::string,std::string> *pm) const override;

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
	   IOContext *ioc,
	   bool buffered) override;
  int aio_read(uint64_t off, uint64_t len, bufferlist *pbl,
	       IOContext *ioc) override;
  int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;

  int write(uint64_t off, bufferlist& bl, bool buffered) override;
  int aio_write(uint64_t off, bufferlist& bl,
		IOContext *ioc,
		bool buffered) override;
  int flush() override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(const std::string& path) override;
  void close() override;
};

#endif
