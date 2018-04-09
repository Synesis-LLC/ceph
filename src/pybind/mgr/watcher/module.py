
import cherrypy
import errno
import time
import json
from mgr_module import MgrModule, CommandResult
from pprint import pformat
import threading
import datetime
import math
import operator

DEFAULT_ACTIVE = True
DEFAULT_DEBUG = False

_global_instance = {'plugin': None}

def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']

class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "watcher status",
            "desc": "Show watcher status",
            "perm": "r",
        },
        {
            "cmd": "watcher on",
            "desc": "Enable monitoring osd latencies",
            "perm": "rw",
        },
        {
            "cmd": "watcher off",
            "desc": "Disable monitoring osd latencies",
            "perm": "rw",
        },
        {
            "cmd": "watcher mute",
            "desc": "Disable osd throttling",
            "perm": "rw",
        },
        {
            "cmd": "watcher unmute",
            "desc": "Enable osd throttling",
            "perm": "rw",
        },
        {
            "cmd": "watcher debug on",
            "desc": "Enable debug",
            "perm": "rw",
        },
        {
            "cmd": "watcher debug off",
            "desc": "Disable debug",
            "perm": "rw",
        },
        {
            "cmd": "watcher cfg set name=key,type=CephString, name=value,type=CephString",
            "desc": "Set config value",
            "perm": "rw",
        },
        {
            "cmd": "watcher cfg reset name=key,type=CephString",
            "desc": "Reset config-key option to default",
            "perm": "rw",
        },
        {
            "cmd": "watcher cfg init",
            "desc": "Initialize config-key options",
            "perm": "rw",
        },
    ]

    def handle_command(self, command):
        self.log.error("Handling command: '%s'" % str(command))
        if command['prefix'] == 'watcher status':
            return (0, json.dumps(self.get_state(), indent=2), '')

        elif command['prefix'] == 'watcher on':
            self.cfg_enable('active')
            return (0, '', '')

        elif command['prefix'] == 'watcher off':
            self.cfg_disable('active')
            return (0, '', '')

        elif command['prefix'] == 'watcher mute':
            self.cfg_enable('dry_run')
            return (0, '', '')

        elif command['prefix'] == 'watcher unmute':
            self.cfg_disable('dry_run')
            return (0, '', '')

        elif command['prefix'] == 'watcher debug on':
            self.cfg_enable('enable_debug')
            return (0, '', '')

        elif command['prefix'] == 'watcher debug off':
            self.cfg_disable('enable_debug')
            return (0, '', '')

        elif command['prefix'] == 'watcher cfg set':
            key = str(command['key'])
            value = str(command['value'])
            if key in self.cfg:
                self.cfg_set_str(key, value)
                return (0, '', '')
            else:
                return (-errno.EINVAL, '', 'key "%s" not found in config' % key)

        elif command['prefix'] == 'watcher cfg reset':
            key = str(command['key'])
            if key in self.cfg:
                self.cfg_reset(key)
                return (0, '', '')
            else:
                return (-errno.EINVAL, '', 'key "%s" not found in config' % key)

        elif command['prefix'] == 'watcher cfg init':
            self.cfg_init()
            return (0, '', '')

        else:
            return (-errno.EINVAL, '', "Command not found '{0}'".format(command['prefix']))

    config_options = {
        'server_addr' : (str, '::'),
        'server_port' : (int, 9284),

        'active' : (bool, DEFAULT_ACTIVE),
        'enable_debug' : (bool, DEFAULT_DEBUG),
        'dry_run' : (bool, False),

        # process stats every 5 seconds
        'period' : (int, 5),
        # width of window in periods
        # if stats not contain osd then collected stats will be deleted
        # so there is no need to control timestamps of stats values
        'window_width' : (int, 5*12),

        'dump_latency' : (bool, False),
        'dump_latency_filename' : (str, "/tmp/ceph_osd_latencies.json"),

        'default_device_class' : (str, 'hdd'),

        'hdd' : {
            'pri_aff' : {
                'active' : (bool, True),
                'max_compliant_latency' : (float, 50),
                'avg_latency_threshold' : (float, 5.0),
                'min_latency_threshold' : (float, 1.0),
                'latest_latency_threshold' : (float, 5.0),
                'max_throttled_osds' : (float, 0.05),
                'max_osds_sanity_check' : (float, 0.1),
            },
            'out' : {
                'active' : (bool, False),
                'max_compliant_latency' : (float, 50),
                'avg_latency_threshold' : (float, 10.0),
                'min_latency_threshold' : (float, 2.0),
                'latest_latency_threshold' : (float, 10.0),
                'max_throttled_osds' : (float, 0.02),
                'max_osds_sanity_check' : (float, 0.1),
            }
        },

        'ssd' : {
            'pri_aff' : {
                'active' : (bool, True),
                'max_compliant_latency' : (float, 5),
                'avg_latency_threshold' : (float, 5.0),
                'min_latency_threshold' : (float, 1.0),
                'latest_latency_threshold' : (float, 5.0),
                'max_throttled_osds' : (float, 0.05),
                'max_osds_sanity_check' : (float, 0.1),
            },
            'out' : {
                'active' : (bool, False),
                'max_compliant_latency' : (float, 5),
                'avg_latency_threshold' : (float, 10.0),
                'min_latency_threshold' : (float, 2.0),
                'latest_latency_threshold' : (float, 10.0),
                'max_throttled_osds' : (float, 0.02),
                'max_osds_sanity_check' : (float, 0.1),
            }
        }
    }

    def cfg_enable(self, key):
        self.cfg[key] = True
        self._cfg_write(key, True)

    def cfg_disable(self, key):
        self.cfg[key] = False
        self._cfg_write(key, False)

    def get_conf_spec(self, key):
        keys = key.split('.')
        spec = self.config_options
        for k in keys:
            if isinstance(spec, dict) and k in spec:
                spec = spec[k]
            else:
                return None
        return spec

    def cfg_get(self, *keys):
        key = '.'.join(keys)
        return self.cfg.get(key, None)

    def cfg_get_section(self, *section_path):
        prefix = '.'.join(section_path)
        section = {}
        for k,v in self.cfg.items():
            if k.startswith(prefix):
                key = k[len(prefix)+1:]
                section[key] = v
        return section

    def _cfg_write(self, key, value):
        if key in self.cfg:
            if isinstance(value, bool):
                value = '1' if value else ''
            else:
                value = str(value)
            self.set_config(key, value)

    def cfg_set_str(self, key, str_value):
        if key in self.cfg:
            spec = self.get_conf_spec(key)
            if spec is not None:
                self.cfg[key] = spec[0](str_value)
                self.set_config(key, str_value)

    def cfg_reset(self, key):
        if key in self.cfg:
            spec = self.get_conf_spec(key)
            if spec is not None:
                self.cfg[key] = spec[1]
                self._cfg_write(key, spec[1])

    def list_config_options(self, root=None, prefix=None):
        if root is None:
            root = self.config_options

        d = {}
        for k,v in root.items():
            if prefix is not None:
                key = prefix + '.' + k
            else:
                key = k
            if isinstance(v, dict):
                d1 = self.list_config_options(v, key)
                for k1,v1 in d1.items():
                    d[k1] = v1
            else:
                d[key] = v
        return d

    def cfg_init(self):
        self.log.info('Initializing config options')

        for key,value in self.cfg.items():
            v = self.get_config(key, None)
            if v is None or value != v:
                self._cfg_write(key, value)

        for key,value in self.cfg.items():
            self.log.info('cfg: %s = %s' % (key, pformat(value)))

    def load_config(self):
        for key,spec in self.list_config_options().items():
            value = self.get_config(key, None)
            if value is None:
                self.cfg[key] = spec[1]
            else:
                self.cfg[key] = spec[0](value)

        for key,value in self.cfg.items():
            self.log.info('cfg: %s = %s' % (key, pformat(value)))


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.cfg = {
            'enable_debug': DEFAULT_DEBUG,
            'active': DEFAULT_ACTIVE,
        }
        self.serving = False
        self.debug_str = "starting"
        self.elapsed = datetime.timedelta()
        self.window = {}
        self.avg_lats = {}
        self.osd_state = {}
        self.slow_osds_by_class = {}
        _global_instance['plugin'] = self


    def do_osd_primary_afinity_sync(self, osd_id, value):
        result = CommandResult("")
        self.send_command(result, "mon", "",
                           json.dumps({
                               "prefix": "osd primary-affinity",
                               "id": osd_id,
                               "weight": value,
                           }),
                           "")
        r, outb, outs = result.wait()


    def do_osd_out_sync(self, osd_id):
        result = CommandResult("")
        self.send_command(result, "mon", "",
                           json.dumps({
                               "prefix": "osd reweight",
                               "id": osd_id,
                               "weight": 0.0,
                           }),
                           "")
        r, outb, outs = result.wait()


    def debug_start(self):
        if self.cfg['enable_debug']:
            self.debug_str = ""
        else:
            self.debug_str = "debug disabled"

    def debug(self, obj, label=""):
        if self.cfg['enable_debug']:
            self.debug_str = label + "\n" + pformat(obj) + "\n" + "-" * 100 + "\n" + self.debug_str


    def collect_stats(self):
        self.osd_map_crush = self.get("osd_map_crush")
        self.osd_stats = self.get("osd_stats")
        self.osd_map = self.get("osd_map")

        if 'devices' not in self.osd_map_crush or len(self.osd_map_crush['devices']) == 0:
            return False
        if 'osd_stats' not in self.osd_stats or len(self.osd_stats['osd_stats']) == 0:
            return False
        if 'osds' not in self.osd_map or len(self.osd_map['osds']) == 0:
            return False

        try:
            self.osd_apply_lats = dict([(osd['osd'], osd['perf_stat']['apply_latency_ms']) for osd in self.osd_stats['osd_stats']
                                        if 'osd' in osd and 'perf_stat' in osd and 'apply_latency_ms' in osd['perf_stat']])
            self.osd_commit_lats = dict([(osd['osd'], osd['perf_stat']['commit_latency_ms']) for osd in self.osd_stats['osd_stats']
                                        if 'osd' in osd and 'perf_stat' in osd and 'commit_latency_ms' in osd['perf_stat']])
        except Exception as e:
            self.debug(e, "collect_stats")
            self.log.error(e.message)
            return False

        if self.cfg['dump_latency']:
            with open(self.cfg['dump_latency_filename'], "a+") as dumpfile:
                dumpfile.write(json.dumps({"apply":self.osd_apply_lats, "commit":self.osd_commit_lats}))
                dumpfile.write("\n")

        self.osd_lats = {}
        for osd_id in set(self.osd_apply_lats.keys()) | set(self.osd_commit_lats.keys()):
            self.osd_lats[osd_id] = max(self.osd_apply_lats.get(osd_id, 0), self.osd_commit_lats.get(osd_id, 0))

        self.log.error("collected osd lats %d" % len(self.osd_lats))

        return True


    def aggregate_stats(self):
        self.osd_class = {}
        try:
            self.osd_class = dict([(osd['id'], osd['class']) for osd in self.osd_map_crush['devices']
                                   if 'class' in osd and 'id' in osd])
            #self.debug(self.osd_class, 'device class')
        except Exception as e:
            self.debug(e, "aggregate_stats 1")
            self.log.error(e.message)
            return False

        try:
            osd_state = dict([(osd['osd'], {'up':osd['up'], 'in':osd['in'], 'primary_affinity':osd['primary_affinity']}) for osd in self.osd_map['osds']])
            self.osd_state = {}
            for osd_id, osd in osd_state.items():
                c = self.osd_class[osd_id]
                if c not in self.osd_state:
                    self.osd_state[c] = {}
                self.osd_state[c][osd_id] = osd
            #self.debug(self.osd_state)
        except Exception as e:
            self.debug(e, "aggregate_stats 2")
            self.log.error(e.message)
            return False

        self.log.error("osd by class: " + ", ".join(["%s:%d" % (c, len(v)) for c,v in self.osd_state.items()]))

        # update latency window
        # {class -> {osd_id -> [lats]}}
        for osd,c in self.osd_class.items():
            if c not in self.window:
                self.log.error("new class: %s" % c)
                self.window[c] = {}
            if osd not in self.window[c]:
                self.log.error("new osd: %d" % osd)
                self.window[c][osd] = []
            self.window[c][osd].append(self.osd_lats[osd])
            if len(self.window[c][osd]) > self.cfg['window_width']:
                self.window[c][osd] = self.window[c][osd][-self.cfg['window_width']:]

        # cleanup window from removed osds
        for c,v in self.window.items():
            to_remove = []
            for osd in self.window[c]:
                if osd not in self.osd_lats:
                    to_remove.append(osd)
            for osd in to_remove:
                self.log.error("drop osd-%d window" % osd)
                del self.window[c][osd]

        if self.cfg['enable_debug']:
            debug_window = {}
            for dev_type,dev_type_data in self.window.items():
                debug_window[dev_type] = dict([(osd_id, (len(values), sum(values), min(values), max(values))) for osd_id,values in dev_type_data.items()])
            self.debug(debug_window, "window")

        # calculate sum latencies
        self.avg_lats = {}
        self.min_lats = {}
        self.max_lats = {}
        self.latest_lats = {}
        continue_process = False
        for osd,c in self.osd_class.items():
            if c not in self.avg_lats:
                self.avg_lats[c] = {}
                self.min_lats[c] = {}
                self.max_lats[c] = {}
                self.latest_lats[c] = {}
            # prevent reweights before collected stats, for example on startup of plugin or osd
            if osd not in self.window[c]:
                continue
            if len(self.window[c][osd]) == self.cfg['window_width']:
                self.avg_lats[c][osd] = float(sum(self.window[c][osd])) / len(self.window[c][osd])
                self.min_lats[c][osd] = float(min(self.window[c][osd]))
                self.max_lats[c][osd] = float(max(self.window[c][osd]))
                self.latest_lats[c][osd] = float(self.window[c][osd][-1])
                continue_process = True

        return continue_process


    def check_cluster_state(self):
        self.pg_status = self.get("pg_status")
        unknown = self.pg_status.get('unknown_pgs_ratio', 0.0)
        degraded = self.pg_status.get('degraded_ratio', 0.0)
        inactive = self.pg_status.get('inactive_pgs_ratio', 0.0)
        misplaced = self.pg_status.get('misplaced_ratio', 0.0)
        self.log.error('unknown %f degraded %f inactive %f misplaced %g',
                       unknown, degraded, inactive, misplaced)
        if unknown > 0.0:
            self.log.error('Some PGs (%f) are unknown; waiting', unknown)
            return False
        elif degraded > 0.0:
            self.log.error('Some objects (%f) are degraded; waiting', degraded)
            return False
        elif inactive > 0.0:
            self.log.error('Some PGs (%f) are inactive; waiting', inactive)
            return False
        elif misplaced > 0.0:
            self.log.error('Some PGs (%f) are misplaced; waiting', misplaced)
            return False

        return True

    def filter_osds(self):
        osds = {}
        for c,v in self.avg_lats.items():
            osds[c] = []
            for osd,_ in v.items():
                if self.osd_state[c][osd]['in'] == 1 and self.osd_state[c][osd]['up'] == 1:
                    osds[c].append(osd)
        return osds


    def find_slow_osds(self, c, osds, params, throttled_osds):
        if len(osds) < 3:
            return []

        active = params['active']
        if not active:
            return []

        avg_lat = sum(self.avg_lats[c].values()) / len(self.avg_lats[c])
        max_avg_lat = max(self.avg_lats[c].values())
        max_lat_osd = max(self.max_lats[c].items(), key=operator.itemgetter(1))

        max_compliant_latency = params['max_compliant_latency']
        avg_latency_threshold = params['avg_latency_threshold'] * avg_lat
        min_latency_threshold = params['min_latency_threshold'] * avg_lat
        latest_latency_threshold = params['latest_latency_threshold'] * avg_lat

        self.debug((avg_lat, max_compliant_latency, avg_latency_threshold, min_latency_threshold, latest_latency_threshold), "find_slow_osds")
        self.log.error('%s: avg_latency=%f, max_avg_lat=%f, max_lat=%f(osd-%d)' % (c, avg_lat, max_avg_lat, max_lat_osd[1], max_lat_osd[0]))

        throttles = []
        for osd,lat in self.avg_lats[c].items():
            if lat <= max_compliant_latency:
                continue
            min_latency = self.min_lats[c][osd]
            latest = self.latest_lats[c][osd]
            if lat > avg_latency_threshold and min_latency > min_latency_threshold and latest > latest_latency_threshold:
                self.log.error('%s: osd-%d: latency: avg=%f/%f, min=%f/%f, current=%f/%f' % \
                                (c, osd, lat, avg_latency_threshold, min_latency, min_latency_threshold, latest, latest_latency_threshold))
                throttles.append(osd)

        if len(throttles) == 0:
            self.log.error('%s: all osds latencies are under thresholds' % c)
            return []

        all_osds = self.osd_state[c]
        sanity_check_max_osds = int(len(all_osds)*params['max_osds_sanity_check'])

        if len(throttles) > sanity_check_max_osds:
            msg = "%s: throttling algorithm found too many osds %d than allowed %d, please verify module configuration." % \
                    (c, len(throttles), sanity_check_max_osds)
            self.debug(throttles, msg)
            self.log.error(msg)
            return []

        max_throttled_osds = int(len(all_osds)*params['max_throttled_osds'])

        if len(throttled_osds) >= max_throttled_osds:
            self.log.error('%s: max_throttled_osds already throttled: %d >= %d' % (c, len(throttled_osds), max_throttled_osds))
            return []

        to_throttle_osds = set(throttles) - set(throttled_osds)
        max_throttled_osds -= len(throttled_osds)
        self.log.error('%s: allowed to throttle %d osds' % (c, max_throttled_osds))
        if max_throttled_osds > 0:
            return list(to_throttle_osds)[:max_throttled_osds]
        else:
            return []


    def recalculate_throttling(self):
        osds_by_class = self.filter_osds()
        self.debug(osds_by_class, 'filtered osds')

        self.osds_to_trottle = []
        self.osds_to_out = []
        for c,osds in osds_by_class.items():
            all_osds = self.osd_state[c]
            out_osds = [osd_id for osd_id, osd in all_osds.items() if osd['in'] == 0]
            throttled_osds = [osd_id for osd_id, osd in all_osds.items() if osd['primary_affinity'] == 0.0]

            osds_to_trottle = self.find_slow_osds(c, osds, self.cfg_get_section(c, 'pri_aff'), set(throttled_osds) | set(out_osds))

            osds_to_out = self.find_slow_osds(c, osds, self.cfg_get_section(c, 'out'), out_osds)

            tmp = {'throttled_osds': list(throttled_osds),
                    'out_osds': list(out_osds)}
            self.debug(tmp, "throttling state")
            self.log.error("%s: throttling state: %s" % (c, json.dumps(tmp)))
            self.log.error("%s: selected osds: pri_aff: %s, out: %s" % (c, json.dumps(osds_to_trottle), json.dumps(osds_to_out)))

            self.osds_to_trottle += osds_to_trottle
            self.osds_to_out += osds_to_out

        return len(self.osds_to_trottle) > 0 or len(self.osds_to_out) > 0


    def apply_throttling(self):
        for osd_id in self.osds_to_trottle:
            self.log.error('drop primary affinity to 0.0 for osd-%d' % osd_id)
            self.do_osd_primary_afinity_sync(osd_id, 0.0)
            self.log.error("drop osd-%d window" % osd_id)
            del self.window[self.osd_class[osd_id]][osd_id]

        for osd_id in self.osds_to_out:
            self.log.error('throw out osd-%d' % osd_id)
            self.do_osd_out_sync(osd_id)
            self.log.error("drop osd-%d window" % osd_id)
            del self.window[self.osd_class[osd_id]][osd_id]


    def get_state(self):
        return {
            "elapsed": pformat(self.elapsed),
            "serving": self.serving,
            "slow_osd_by_class": self.slow_osds_by_class,
            "osd_state": self.osd_state,
            "avg_lats": self.avg_lats
        }


    def process(self):
        self.log.info("start process osds")
        self.tick = datetime.datetime.now()
        while self.serving:
            next_tick = self.tick + datetime.timedelta(seconds=self.cfg['period'])
            while datetime.datetime.now() < next_tick and self.serving:
                time.sleep(0.1)
            if not self.serving:
                break

            self.debug_start()
            self.debug(self.cfg, 'config')

            self.tick = datetime.datetime.now()
            if not self.cfg['active']:
                self.window = {}
                continue

            try:
                if self.check_cluster_state():
                    if self.collect_stats():
                        if self.aggregate_stats():
                            if self.recalculate_throttling() and not self.cfg['dry_run']:
                                self.apply_throttling()
                    else:
                        self.window = {}
            except Exception as e:
                self.debug(e, "process")
                self.log.error(e.message)
            self.elapsed = datetime.datetime.now() - self.tick
            self.debug(self.elapsed, "elapsed")


    def shutdown(self):
        cherrypy.engine.exit()
        self.serving = False
        self.worker.join()
        pass


    def serve(self):

        class Root(object):

            # collapse everything to '/'
            def _cp_dispatch(self, vpath):
                cherrypy.request.path = ''
                return self

            @cherrypy.expose
            def index(self):
                cherrypy.response.headers['Content-Type'] = 'text/plain'
                return global_instance().debug_str

            @cherrypy.expose(['status', 'state'])
            def status(self):
                cherrypy.response.headers['Content-Type'] = 'application/json'
                return json.dumps(global_instance().get_state(), indent=2)

            @cherrypy.expose(['cfg', 'conf', 'config'])
            def cfg(self):
                cherrypy.response.headers['Content-Type'] = 'application/json'
                return json.dumps(global_instance().cfg, indent=2)

        self.load_config()

        self.serving = True
        self.worker = threading.Thread(target=lambda : self.process())
        self.worker.start()

        self.log.error("server_addr: %s, server_port: %s" % (self.cfg['server_addr'], self.cfg['server_port']))

        cherrypy.config.update({
            'server.socket_host': self.cfg['server_addr'],
            'server.socket_port': self.cfg['server_port'],
            'engine.autoreload.on': False
        })
        cherrypy.tree.mount(Root(), "/")
        cherrypy.engine.start()
        cherrypy.engine.block()

