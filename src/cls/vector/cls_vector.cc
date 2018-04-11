// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 *
 *
 *
 */

#include <algorithm>
#include <string>
#include <sstream>
#include <errno.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(vector)


#include <cmath>

double disatance(const uint8_t* v1, const uint8_t* v2, uint32_t length)
{
  const uint8_t* v1_end = v1 + length;
  double sum = 0;
  while (v1 < v1_end) {
    double d = *v1 / 256.0 - *v2 / 256.0;
    sum += d*d;
    ++v1;
    ++v2;
  }
  return std::sqrt(sum);
}

#define READ_BYTES (1024*1024)

//TODO: signed/unsigned 8, 16, 32, 64 bit integer, 32,64 bit float
//TODO: xattr vector format, example: i32x1024, u8x256, f64x512
//TODO: binary reques/response format
//TODO: find n closest, multiple search request

//TODO: reload class on-fly with osd command with optional requests wait(with timeout) or fail
//TODO: custom osd classes in separate deb package

static int find_closest(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // get lnegth of vectors in object from xattr "length"
  bufferlist attr_bl;
  int err = cls_cxx_getxattr(hctx, "length", &attr_bl);
  if (err < 0) {
    out->append("error reading object \"length\" xattr");
    return err;
  }
  uint32_t length = 0;
  if (attr_bl.length() != sizeof(length)) {
    out->append("object has invalid \"length\" xattr");
    return -EINVAL;
  }
  attr_bl.copy(0, sizeof(length), (char*)&length);

  // check request vector's length match object's
  if (in->length() != length) {
    out->append("object has different \"length\" with request");
    return -EINVAL;
  }

  // decode request vector
  uint8_t request_v[length];
  in->copy(0, length, (char*)request_v);

  uint64_t obj_size = 0;
  time_t mtime;
  err = cls_cxx_stat(hctx, &obj_size, &mtime);  if (err < 0) {
    out->append("error get object stat");
    return err;
  }
  if (obj_size == 0) {
    out->append("empty object");
    return -EINVAL;
  }

  uint32_t read_length = READ_BYTES / length;
  read_length *= length;
  uint64_t read_count = (obj_size / read_length) + 1;
  int obj_offset = 0;
  uint8_t result[length];
  double min = std::numeric_limits<double>::max();
  while (read_count--) {
    // read READ_BYTES into memory each time
    bufferlist data_bl;
    err = cls_cxx_read(hctx, obj_offset, read_length, &data_bl);
    if (err < 0) {
      out->append("read error");
      return -EIO;
    }
    if (data_bl.length() < length) {
      break;
    }
    obj_offset += data_bl.length();

    // foreach vector in readed data compare disatance with min distance
    int count = data_bl.length() / length;
    int data_bl_offset = 0;
    uint8_t v[length];
    int result_offset = -1;
    while (count--) {
      data_bl.copy(data_bl_offset, length, (char*)v);
      double d = disatance(request_v, v, length);
      if (d < min) {
        min = d;
        result_offset = data_bl_offset;
      }
      data_bl_offset += length;
    }
    if (result_offset >= 0) {
      data_bl.copy(result_offset, length, (char*)result);
    }
  }

  uint32_t dist = std::round(min*256.0);
  out->append((char*)&dist, sizeof(dist));
  out->append((char*)result, length);

  return 0;
}


CLS_INIT(vector)
{
  CLS_LOG(0, "loading cls_vector");

  cls_handle_t h_class;
  cls_method_handle_t h_find_closest;

  cls_register("vector", &h_class);

  cls_register_cxx_method(h_class, "find_closest", CLS_METHOD_RD, find_closest, &h_find_closest);
}

