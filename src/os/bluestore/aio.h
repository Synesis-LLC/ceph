// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "acconfig.h"
#ifdef HAVE_LIBAIO
# include <libaio.h>

#include <boost/intrusive/list.hpp>
#include <boost/container/small_vector.hpp>

#include "include/buffer.h"
#include "include/types.h"

#include <chrono>
#include <atomic>

struct aio_t {
  struct iocb iocb;  // must be first element; see shenanigans in aio_queue_t
  void *priv;
  int fd;
  boost::container::small_vector<iovec,4> iov;
  uint64_t offset, length;
  long rval;
  bufferlist bl;  ///< write payload (so that it remains stable for duration)

  boost::intrusive::list_member_hook<> queue_item;

  aio_t(void *p, int f) : priv(p), fd(f), offset(0), length(0), rval(-1000) {
  }

  void pwritev(uint64_t _offset, uint64_t len) {
    offset = _offset;
    length = len;
    io_prep_pwritev(&iocb, fd, &iov[0], iov.size(), offset);
  }
  void pread(uint64_t _offset, uint64_t len) {
    offset = _offset;
    length = len;
    bufferptr p = buffer::create_page_aligned(length);
    io_prep_pread(&iocb, fd, p.c_str(), length, offset);
    bl.append(std::move(p));
  }

  long get_return_value() {
    return rval;
  }
};

typedef boost::intrusive::list<
  aio_t,
  boost::intrusive::member_hook<
    aio_t,
    boost::intrusive::list_member_hook<>,
    &aio_t::queue_item> > aio_list_t;

struct aio_queue_t {
  io_context_t ctx;
  size_t max_iodepth = 0;

  typedef list<aio_t>::iterator aio_iter;

  explicit aio_queue_t() : ctx(0),
    ops_in_flight(0)
  {}

  ~aio_queue_t() {
    assert(ctx == 0);
  }

  typedef std::chrono::steady_clock ops_clock_t;
  std::atomic<int64_t> ops_in_flight;
  ops_clock_t::time_point last_op_timestamp = ops_clock_t::now();

  struct aio_queue_state_t {
    int64_t ops_in_flight;
    int64_t elapsed_from_last_op_us;
  };

  aio_queue_state_t get_aio_state() {
    aio_queue_state_t r;
    // first syncronized load value of in_flight counter
    // then read value of op timestamp
    r.ops_in_flight = ops_in_flight.load(std::memory_order_relaxed);
    if (r.ops_in_flight > 0) {
      std::atomic_thread_fence(std::memory_order_acquire);
      r.elapsed_from_last_op_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
            ops_clock_t::now() - last_op_timestamp
          ).count();
    } else {
      // no running ops (ops_in_flight <= 0)
      r.ops_in_flight = 0;
      r.elapsed_from_last_op_us = 0;
    }
    return r;
  }

  int init(size_t _max_iodepth) {
    max_iodepth = _max_iodepth;
    assert(max_iodepth > 0);
    assert(ctx == 0);
    int r = io_setup(max_iodepth, &ctx);
    if (r < 0) {
      if (ctx) {
	io_destroy(ctx);
	ctx = 0;
      }
    }
    return r;
  }
  void shutdown() {
    if (ctx) {
      int r = io_destroy(ctx);
      assert(r == 0);
      ctx = 0;
    }
  }

  int submit(aio_t &aio, int *retries);
  int submit_batch(aio_iter begin, aio_iter end, uint16_t aios_size, 
		   void *priv, int *retries);
  int get_next_completed(int timeout_ms, aio_t **paio, int max);
};

#endif
