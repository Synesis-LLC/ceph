// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <set>
#include <string>

#include <boost/utility/string_ref.hpp>

#include "rgw_frontend.h"
#include "rgw_client_io_filters.h"

#define dout_subsys ceph_subsys_rgw

void* RGWCivetWebFrontend::civetweb_metrics_thread(void *arg)
{
    RGWCivetWebFrontend *fe = static_cast<RGWCivetWebFrontend *>(arg);

    while (1) {
        fe->process_metrics();
        sleep(1);
    }
}

void RGWCivetWebFrontend::run_metrics_thread()
{
    int ret = pthread_create(&metrics_thread_id, NULL, civetweb_metrics_thread, this);
    if (ret != 0) {
        metrics_thread_id = 0;
    }

    dout(20) << "RGWCivetWebFrontend::run_metrics_thread: " << ((ret == 0) ? "success" : "error") << dendl;
}

int parse_int_from_json(char *buf, char *collection, const char *variable)
{
    int ret = -1;

    char *ptr = strstr(buf, collection);
    if (ptr != NULL) {
        ptr += strlen(collection);

        ptr = strstr(ptr, variable);
        if (ptr != NULL) {
            ptr += strlen(variable);

            for ( ; *ptr != '\0' ; ++ptr) {
                if (*ptr >= '0' && *ptr <= '9') {
                    ret = atoi(ptr);
                    break;
                }
            }
        }
    }

    return ret;
}

void RGWCivetWebFrontend::process_metrics()
{
    char buf[1024] = "";

    int ret = mg_get_context_info(ctx, buf, sizeof(buf)-1);
    if (ret <= 0 || ret >= sizeof(buf)) {
        dout(10) << "RGWCivetWebFrontend::process_metrics: mg_get_context_info failed" << dendl;
        return;
    }    
    buf[ret] = '\0';
    // dout(20) << "RGWCivetWebFrontend::process_metrics: mg_get_context_info buf: " << buf << dendl;

    auto parse_int_from_json = [&buf](const char *collection, const char *variable) mutable -> int
    {
        int ret = -1;

        char *ptr = strstr(buf, collection);
        if (ptr != NULL) {
            ptr += strlen(collection);

            ptr = strstr(ptr, variable);
            if (ptr != NULL) {
                ptr += strlen(variable);

                for ( ; *ptr != '\0' ; ++ptr) {
                    if (*ptr >= '0' && *ptr <= '9') {
                        ret = atoi(ptr);
                        break;
                    }
                }
            }
        }

        return ret;
    };

    int con_active = parse_int_from_json((const char *)"connections", (const char *)"active");
    int con_maxactive = parse_int_from_json((const char *)"connections", (const char *)"maxActive");
    int con_total = parse_int_from_json((const char *)"connections", (const char *)"total");

    // dout(20) << "RGWCivetWebFrontend::process_metrics: conn active: " << con_active << " con_maxactive: " << con_maxactive << " con total: " << con_total << dendl;

    perfcounter->set(l_rgw_con_active, con_active);
    perfcounter->set(l_rgw_con_maxactive, con_maxactive);
    perfcounter->set(l_rgw_con_total, con_total);
}

void RGWCivetWebFrontend::stop_metrics_thread()
{
    int ret = 0;

    if (metrics_thread_id > 0) {
        ret = pthread_cancel(metrics_thread_id);
        metrics_thread_id = 0;
    }

    dout(20) << "RGWCivetWebFrontend::stop_metrics_thread: " << ((ret == 0) ? "success" : "error") << dendl;
}

static int civetweb_callback(struct mg_connection* conn)
{
  const struct mg_request_info* const req_info = mg_get_request_info(conn);
  return static_cast<RGWCivetWebFrontend *>(req_info->user_data)->process(conn);
}

int RGWCivetWebFrontend::process(struct mg_connection*  const conn)
{
  /* Hold a read lock over access to env.store for reconfiguration. */
  RWLock::RLocker lock(env.mutex);

  RGWCivetWeb cw_client(conn);
  auto real_client_io = rgw::io::add_reordering(
                          rgw::io::add_buffering(dout_context,
                            rgw::io::add_chunking(
                              rgw::io::add_conlen_controlling(
                                &cw_client))));
  RGWRestfulIO client_io(dout_context, &real_client_io);

  RGWRequest req(env.store->get_new_req_id());
  int http_ret = 0;
  int ret = process_request(env.store, env.rest, &req, env.uri_prefix,
                            *env.auth_registry, &client_io, env.olog, &http_ret);
  if (ret < 0) {
    /* We don't really care about return code. */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  if (http_ret <= 0) {
    /* Mark as processed. */
    return 1;
  }

  return http_ret;
}

int RGWCivetWebFrontend::run()
{
  auto& conf_map = conf->get_config_map();

  set_conf_default(conf_map, "num_threads",
                   std::to_string(g_conf->rgw_thread_pool_size));
  set_conf_default(conf_map, "decode_url", "no");
  set_conf_default(conf_map, "enable_keep_alive", "yes");
  set_conf_default(conf_map, "validate_http_method", "no");
  set_conf_default(conf_map, "canonicalize_url_path", "no");
  set_conf_default(conf_map, "enable_auth_domain_check", "no");

  std::string listening_ports;
  // support multiple port= entries
  auto range = conf_map.equal_range("port");
  for (auto p = range.first; p != range.second; ++p) {
    std::string port_str = p->second;
    // support port= entries with multiple values
    std::replace(port_str.begin(), port_str.end(), '+', ',');
    if (!listening_ports.empty()) {
      listening_ports.append(1, ',');
    }
    listening_ports.append(port_str);
  }
  if (listening_ports.empty()) {
    listening_ports = "80";
  }
  conf_map.emplace("listening_ports", std::move(listening_ports));

  /* Set run_as_user. This will cause civetweb to invoke setuid() and setgid()
   * based on pw_uid and pw_gid obtained from pw_name. */
  std::string uid_string = g_ceph_context->get_set_uid_string();
  if (! uid_string.empty()) {
    conf_map.emplace("run_as_user", std::move(uid_string));
  }

  /* Prepare options for CivetWeb. */
  const std::set<boost::string_ref> rgw_opts = { "port", "prefix" };

  std::vector<const char*> options;

  for (const auto& pair : conf_map) {
    if (! rgw_opts.count(pair.first)) {
      /* CivetWeb doesn't understand configurables of the glue layer between
       * it and RadosGW. We need to strip them out. Otherwise CivetWeb would
       * signalise an error. */
      options.push_back(pair.first.c_str());
      options.push_back(pair.second.c_str());

      dout(20) << "civetweb config: " << pair.first
               << ": " << pair.second << dendl;
    }
  }

  options.push_back(nullptr);
  /* Initialize the CivetWeb right now. */
  struct mg_callbacks cb;
  memset((void *)&cb, 0, sizeof(cb));
  cb.begin_request = civetweb_callback;
  cb.log_message = rgw_civetweb_log_callback;
  cb.log_access = rgw_civetweb_log_access_callback;
  cb.log_err_access = rgw_civetweb_log_err_access_callback;
  ctx = mg_start(&cb, this, options.data());

  /* Run metrics thread */
  run_metrics_thread();

  return ! ctx ? -EIO : 0;
} /* RGWCivetWebFrontend::run */
