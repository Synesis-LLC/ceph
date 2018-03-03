
import cherrypy
import time
import json
from mgr_module import MgrModule, CommandResult
import pprint
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
    ]

    def handle_command(self, command):
        self.log.error("Handling command: '%s'" % str(command))
        if command['prefix'] == 'watcher status':
            return (0, json.dumps(self.get_state(), indent=2), '')

        elif command['prefix'] == 'watcher on':
            if not self.cfg['active']:
                self.set_config('active', '1')
                self.cfg['active'] = True
            return (0, '', '')

        elif command['prefix'] == 'watcher off':
            if self.cfg['active']:
                self.set_config('active', '')
                self.cfg['active'] = False
            return (0, '', '')

        elif command['prefix'] == 'watcher mute':
            if not self.cfg['dry_run']:
                self.set_config('dry_run', '1')
                self.cfg['dry_run'] = True
            return (0, '', '')

        elif command['prefix'] == 'watcher unmute':
            if self.cfg['dry_run']:
                self.set_config('dry_run', '')
                self.cfg['dry_run'] = False
            return (0, '', '')

        elif command['prefix'] == 'watcher debug on':
            if not self.cfg['enable_debug']:
                self.set_config('enable_debug', '1')
                self.cfg['enable_debug'] = True
            return (0, '', '')

        elif command['prefix'] == 'watcher debug off':
            if self.cfg['enable_debug']:
                self.set_config('enable_debug', '')
                self.cfg['enable_debug'] = False
            return (0, '', '')

        else:
            return (-errno.EINVAL, '', "Command not found '{0}'".format(command['prefix']))

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
        self.sum_lats = {}
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
            self.debug_str = label + "\n" + pprint.pformat(obj) + "\n" + "-" * 100 + "\n" + self.debug_str


    def find_slow_osds(self, lats, min_threshold):
        if len(lats) < 3:
            return {}

        avg_lat = sum(lats.values()) / len(lats)
        latency_limit_mult = self.cfg['latency_limit_mult']
        latency_threshold = max(avg_lat * latency_limit_mult, min_threshold)

        self.debug((avg_lat, latency_limit_mult, min_threshold, latency_threshold, lats), "find_osds_to_throttle")

        throttles = []
        for osd,lat in lats.items():
            if lat > latency_threshold:
                throttles.append(osd)

        return throttles


    def collect_stats(self):
        self.osd_map_crush = self.get("osd_map_crush")
        self.osd_stats = self.get("osd_stats")
        self.osd_map = self.get("osd_map")
        self.pg_status = self.get("pg_status")

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
        
        self.log.error("osd lats %d" % len(self.osd_lats))

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
                self.log.error("drop osd.%d window" % osd)
                del self.window[c][osd]

        if self.cfg['enable_debug']:
            debug_window = {}
            for dev_type,dev_type_data in self.window.items():
                debug_window[dev_type] = dict([(osd_id, (len(values), sum(values), min(values), max(values))) for osd_id,values in dev_type_data.items()])
            self.debug(debug_window, "window")

        # calculate sum latencies
        # {class -> [{osd_id -> sum_apply_lat}, {osd_id -> sum_commit_lat}]}
        self.sum_lats = {}
        continue_process = False
        for osd,c in self.osd_class.items():
            if c not in self.sum_lats:
                self.sum_lats[c] = {}
            # prevent reweights before collected stats, for example on startup of plugin or osd
            if osd not in self.window[c]:
                continue
            if len(self.window[c][osd]) == self.cfg['window_width']:
                self.sum_lats[c][osd] = float(sum(self.window[c][osd]))
                continue_process = True
        #self.debug(self.sum_lats, "sum latency")

        return continue_process


    def recalculate_throttling(self):
        # filter only up and in osds
        # {class -> [{osd_id -> sum_apply_lat}, {osd_id -> sum_commit_lat}]}
        lats = {}
        for c,v in self.sum_lats.items():
            lats[c] = {}
            for osd,lat in v.items():
                if self.osd_state[c][osd]['in'] == 1 and self.osd_state[c][osd]['up'] == 1:
                    lats[c][osd] = lat
        self.debug(lats, 'filtered sum latencies')

        # calculate new throttles with min_latency according to osd class
        self.slow_osds_by_class = {}

        continue_process = False
        for c,v in lats.items():
            min_threshold = {
                "ssd" : self.cfg['max_compliant_latency_ssd'],
                "hdd" : self.cfg['max_compliant_latency_hdd'],
            }.get(c, self.cfg['max_compliant_latency'])
            min_threshold *= self.cfg['window_width']
            self.slow_osds_by_class[c] = self.find_slow_osds(v, min_threshold)
            if len(self.slow_osds_by_class[c]) > 0:
                continue_process = True

        self.debug(self.slow_osds_by_class, 'slow_osds_by_class')
        self.log.error("slow_osds_by_class: %s" % json.dumps(self.slow_osds_by_class))

        if not continue_process:
            return False

        unknown = self.pg_status.get('unknown_pgs_ratio', 0.0)
        degraded = self.pg_status.get('degraded_ratio', 0.0)
        inactive = self.pg_status.get('inactive_pgs_ratio', 0.0)
        misplaced = self.pg_status.get('misplaced_ratio', 0.0)
        self.log.error('unknown %f degraded %f inactive %f misplaced %g',
                       unknown, degraded, inactive, misplaced)
        if unknown > 0.0:
            self.log.error('Some PGs (%f) are unknown; waiting', unknown)
            return
        elif degraded > 0.0:
            self.log.error('Some objects (%f) are degraded; waiting', degraded)
            return
        elif inactive > 0.0:
            self.log.error('Some PGs (%f) are inactive; waiting', inactive)
            return
        elif misplaced > 0.0:
            self.log.error('Some PGs (%f) are misplaced; waiting', misplaced)
            return

        self.osds_to_trottle = []
        self.osds_to_out = []

        for c,slow_osds in self.slow_osds_by_class.items():
            all_osds = self.osd_state[c]
            max_throttled_osds = int(len(all_osds)*self.cfg['max_throttled_osds'])
            sanity_check_max_osds = int(len(all_osds)*self.cfg['max_osds_to_throttle_sanity_check'])
            out_osds = set([osd_id for osd_id, osd in all_osds.items() if osd['in'] == 0])
            throttled_osds = set([osd_id for osd_id, osd in all_osds.items() if osd['primary_affinity'] == 0.0])

            if len(slow_osds) > sanity_check_max_osds:
                msg = "Throttling algorithm found too many \"%s\" osds %d than allowed %d, please verify module configuration." % \
                        (c, len(slow_osds), sanity_check_max_osds)
                self.debug(slow_osds, msg)
                self.log.error(msg)
                continue

            tmp = {'max_throttled_osds': max_throttled_osds,
                    'slow_osds': slow_osds,
                    'throttled_osds': throttled_osds,
                    'out_osds': out_osds}
            self.debug(tmp, "throttling state")
            self.log.error("throttling state: %s" % json.dumps(tmp))

            # Set primary affinity to 0.0 for limited number of slow osds.
            to_throttle_osds = set(slow_osds) - (throttled_osds | out_osds)
            max_throttled_osds -= len(throttled_osds | out_osds)
            if max_throttled_osds > 0:
                self.osds_to_trottle = list(to_throttle_osds)[:max_throttled_osds]

                for osd_id in self.osds_to_trottle:
                    # Drop osd lats windows to collect all new window
                    self.log.error("drop osd.%d window" % osd)
                    del self.window[c][osd_id]
            else:
                self.osds_to_trottle = []

            # Set primary affinity to 0.0 did not help - thorw osd out.
            self.osds_to_out = list(set(slow_osds) & throttled_osds)

            self.debug((self.osds_to_trottle, self.osds_to_out), "throttling attempt")

        return len(self.osds_to_trottle) > 0 or len(self.osds_to_out) > 0


    def apply_throttling(self):
        for osd_id in self.osds_to_trottle:
            self.log.error('drop primary affinity to 0.0 for osd-%d' % osd_id)
            self.do_osd_primary_afinity_sync(osd_id, 0.0)
        
        for osd_id in self.osds_to_out:
            self.log.error('throw out osd-%d' % osd_id)
            self.do_osd_out_sync(osd_id)


    def get_state(self):
        return {
            "elapsed": pprint.pformat(self.elapsed),
            "serving": self.serving,
            "slow_osd_by_class": self.slow_osds_by_class,
            "osd_state": self.osd_state,
            "sum_lats": self.sum_lats
        }


    def ld_cfg(self, name, type=str, default=""):
        self.cfg[name] = type(self.get_config(name, default))

    def load_config(self):
        self.ld_cfg('server_addr', str, '::')
        self.ld_cfg('server_port', int, 9284)

        self.ld_cfg('active', bool, DEFAULT_ACTIVE)
        self.ld_cfg('enable_debug', bool, DEFAULT_DEBUG)
        self.ld_cfg('dry_run', bool, False)

        # process stats every 5 seconds
        self.ld_cfg('period', int, 5)
        # width of window in periods
        # if stats not contain osd then collected stats will be deleted
        # so there is no need to control timestamps of stats values
        self.ld_cfg('window_width', int, 5*12)

        # maximum absolute latency value when osd will not be reweighted    
        self.ld_cfg('max_compliant_latency', float, 50)
        self.ld_cfg('max_compliant_latency_ssd', float, 5)
        self.ld_cfg('max_compliant_latency_hdd', float, 50)

        # Sum of latency on window limit compared to avg
        self.ld_cfg('latency_limit_mult', float, 5.0)

        # limit max number of throttled osds of each 
        self.ld_cfg('max_throttled_osds', float, 0.05)

        self.ld_cfg('max_osds_to_throttle_sanity_check', float, 0.2)

        self.ld_cfg('dump_latency', bool, False)
        self.ld_cfg('dump_latency_filename', str, "/tmp/ceph_osd_latencies.json")


    def process(self):
        self.log.info("start process osds")
        self.tick = datetime.datetime.now()
        while self.serving:
            next_tick = self.tick + datetime.timedelta(seconds=self.cfg['period'])
            while datetime.datetime.now() < next_tick and self.serving:
                time.sleep(0.1)
            if not self.serving:
                break
            self.tick = datetime.datetime.now()
            if not self.cfg['active']:
                self.window = {}
                continue

            self.debug_start()
            self.debug(self.cfg, 'config')
            try:
                if self.collect_stats():
                    if self.aggregate_stats():
                        if self.recalculate_throttling() and not self.cfg['dry_run']:
                            self.apply_throttling()
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

