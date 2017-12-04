// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_op.h"
#include "rgw_bucket.h"
#include "rgw_rest_bucket.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_Bucket_Info : public RGWRESTOp {

public:
  RGWOp_Bucket_Info() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute() override;

  const string name() override { return "get_bucket_info"; }
};

void RGWOp_Bucket_Info::execute()
{
  RGWBucketAdminOpState op_state;

  bool fetch_stats;

  std::string bucket;

  string uid_str;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "stats", false, &fetch_stats);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_fetch_stats(fetch_stats);

  http_ret = RGWBucketAdminOp::info(store, op_state, flusher);
}

class RGWOp_Get_Policy : public RGWRESTOp {

public:
  RGWOp_Get_Policy() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_READ);
  }

  void execute() override;

  const string name() override { return "get_policy"; }
};

void RGWOp_Get_Policy::execute()
{
  RGWBucketAdminOpState op_state;

  std::string bucket;
  std::string object;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object(object);

  http_ret = RGWBucketAdminOp::get_policy(store, op_state, flusher);
}

class RGWOp_Check_Bucket_Index : public RGWRESTOp {

public:
  RGWOp_Check_Bucket_Index() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "check_bucket_index"; }
};

void RGWOp_Check_Bucket_Index::execute()
{
  std::string bucket;

  bool fix_index;
  bool check_objects;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "fix", false, &fix_index);
  RESTArgs::get_bool(s, "check-objects", false, &check_objects);

  op_state.set_bucket_name(bucket);
  op_state.set_fix_index(fix_index);
  op_state.set_check_objects(check_objects);

  http_ret = RGWBucketAdminOp::check_index(store, op_state, flusher);
}

class RGWOp_Bucket_Link : public RGWRESTOp {

public:
  RGWOp_Bucket_Link() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "link_bucket"; }
};

void RGWOp_Bucket_Link::execute()
{
  std::string uid_str;
  std::string bucket;
  std::string bucket_id;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "bucket-id", bucket_id, &bucket_id);

  rgw_user uid(uid_str);
  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);
  op_state.set_bucket_id(bucket_id);

  http_ret = RGWBucketAdminOp::link(store, op_state);
}

class RGWOp_Bucket_Unlink : public RGWRESTOp {

public:
  RGWOp_Bucket_Unlink() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "unlink_bucket"; }
};

void RGWOp_Bucket_Unlink::execute()
{
  std::string uid_str;
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_user_id(uid);
  op_state.set_bucket_name(bucket);

  http_ret = RGWBucketAdminOp::unlink(store, op_state);
}

class RGWOp_Bucket_Remove : public RGWRESTOp {

public:
  RGWOp_Bucket_Remove() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "remove_bucket"; }
};

void RGWOp_Bucket_Remove::execute()
{
  std::string bucket;
  bool delete_children;
  bool bypass_gc;
  bool keep_index_consistent;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_bool(s, "purge-objects", false, &delete_children);
  RESTArgs::get_bool(s, "bypass-gc", false, &bypass_gc);
  RESTArgs::get_bool(s, "keep-index-consistent", true, &keep_index_consistent);

  op_state.set_bucket_name(bucket);
  op_state.set_delete_children(delete_children);
  op_state.set_max_aio(store->ctx()->_conf->rgw_remove_object_max_concurrent_ios);

  http_ret = RGWBucketAdminOp::remove_bucket(store, op_state, bypass_gc, keep_index_consistent);
}

class RGWOp_Object_Remove: public RGWRESTOp {

public:
  RGWOp_Object_Remove() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "remove_object"; }
};

void RGWOp_Object_Remove::execute()
{
  std::string bucket;
  std::string object;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);
  RESTArgs::get_string(s, "object", object, &object);

  op_state.set_bucket_name(bucket);
  op_state.set_object(object);

  http_ret = RGWBucketAdminOp::remove_object(store, op_state);
}

class RGWOp_Bucket_Quota_Set: public RGWRESTOp {

public:
  RGWOp_Bucket_Quota_Set() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "set_bucket_quota_info"; }
};

void RGWOp_Bucket_Quota_Set::execute()
{
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_bucket_name(bucket);

  uint64_t num_value = 0;
  bool exists = false;
  RESTArgs::get_uint64(s, "max_size_kb", num_value, &num_value, &exists);
  if (exists) {
    op_state.quota_changes.max_size = num_value * 1024;
  }
  RESTArgs::get_uint64(s, "max_size", op_state.quota_changes.max_size);
  RESTArgs::get_uint64(s, "max_objects", op_state.quota_changes.max_size);
  RESTArgs::get_bool(s, "enabled", op_state.quota_changes.enabled);
  RESTArgs::get_bool(s, "check_on_raw", op_state.quota_changes.check_on_raw);

  http_ret = RGWBucketAdminOp::set_quota(store, op_state, flusher);
}

class RGWOp_Bucket_Quota_Get: public RGWRESTOp {

public:
  RGWOp_Bucket_Quota_Get() {}

  int check_caps(RGWUserCaps& caps) override {
    return caps.check_cap("buckets", RGW_CAP_WRITE);
  }

  void execute() override;

  const string name() override { return "get_bucket_quota_info"; }
};

void RGWOp_Bucket_Quota_Get::execute()
{
  std::string bucket;

  RGWBucketAdminOpState op_state;

  RESTArgs::get_string(s, "bucket", bucket, &bucket);

  op_state.set_bucket_name(bucket);

  http_ret = RGWBucketAdminOp::get_quota(store, op_state, flusher);
}

RGWOp *RGWHandler_Bucket::op_get()
{

  if (s->info.args.sub_resource_exists("policy"))
    return new RGWOp_Get_Policy;

  if (s->info.args.sub_resource_exists("index"))
    return new RGWOp_Check_Bucket_Index;

  if (s->info.args.sub_resource_exists("quota"))
    return new RGWOp_Bucket_Quota_Get;

  return new RGWOp_Bucket_Info;
}

RGWOp *RGWHandler_Bucket::op_put()
{
  if (s->info.args.sub_resource_exists("quota"))
    return new RGWOp_Bucket_Quota_Set;

  return new RGWOp_Bucket_Link;
}

RGWOp *RGWHandler_Bucket::op_post()
{
  return new RGWOp_Bucket_Unlink;
}

RGWOp *RGWHandler_Bucket::op_delete()
{
  if (s->info.args.sub_resource_exists("object"))
    return new RGWOp_Object_Remove;

  return new RGWOp_Bucket_Remove;
}


