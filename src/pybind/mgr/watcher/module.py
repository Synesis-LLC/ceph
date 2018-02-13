
import cherrypy
import time
import json
from mgr_module import MgrModule, CommandResult
import pprint
import threading
import datetime
import math
import operator

DEFAULT_ADDR = '::'
DEFAULT_PORT = 9284

DEFAULT_ENABLE_DEBUG = True
DEFAULT_DRY_RUN = False
DEFAULT_DUMP_LATENCY = False
DEFAULT_DUMP_LATENCY_FILENAME = "/tmp/ceph_osd_latencies.json"

DEFAULT_PERIOD = 5 # process stats every 5 seconds
DEFAULT_WINDOW = 12*5 # width of window in periods
# if stats not contain osd then collected stats will be deleted
# so there is no need to control timestamps of stats values

# maximum absolute latency value when osd will not be reweighted
DEFAULT_MAX_COMPLIANT_LATENCY = 50
DEFAULT_MAX_COMPLIANT_LATENCY_SSD = 5
DEFAULT_MAX_COMPLIANT_LATENCY_HDD = 50

# Sum of latency on window limit compared to avg
DEFAULT_LATENCY_LIMIT_MULT = 5.0

# limit max number of throttled osds
DEFAULT_MAX_THROTTLED_OSDS = 0.15


_global_instance = {'plugin': None}

def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']

class Module(MgrModule):

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.cfg = {
            'enable_debug': DEFAULT_ENABLE_DEBUG
        }
        self.serving = False
        self.metrics = dict()
        self.debug_str = "started"
        self.elapsed = datetime.timedelta()
        self.window = {}
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


    def do_osd_out_sync(self, osd_id, value):
        result = CommandResult("")
        self.send_command(result, "mon", "",
                           json.dumps({
                               "prefix": "osd primary-affinity",
                               "id": osd_id,
                               "weight": value,
                           }),
                           "")
        r, outb, outs = result.wait()


    def debug_start(self):
        if self.cfg['enable_debug']:
            self.debug_str = ""
        else:
            self.debug_str = "disabled"


    def debug(self, obj, label=""):
        if self.cfg['enable_debug']:
            self.debug_str = label + "\n" + pprint.pformat(obj) + "\n" + "-" * 100 + "\n" + self.debug_str
        else:
            self.debug_str = "disabled"


    def get_new_weights(self, lats, min_threshold):
        if len(lats) < 3:
            return {}

        #TODO: collect set of lats and draw distribution of lats and distribution of distancies
        #TODO: move algorythms to separate files, implement framework for alg testing on collected data
        #TODO: gmm_mle + clustering
        #TODO: 80% closest to median
        neighbours = get_nearest(lats, self.cfg['distance_threshold'])
        #self.debug(neighbours, "neighbours")
        clusters = get_clusters(neighbours)
        #self.debug(clusters, "clusters")
        dominant = max(clusters, key=len)
        #self.debug(dominant, "dominant")

        dominant_lats = dict([(osd,lats[osd]) for osd in dominant])
        dominant_min = min(dominant_lats.values())
        # append all that lower for compliant stddev
        for osd,lat in lats.items():
            if lat < dominant_min and osd not in dominant_lats:
                dominant_lats[osd] = lat
        #self.debug(dominant_lats, "dominant lats")

        mean, std = get_mean_std(dominant_lats.values())
        lat_threshold = mean*self.cfg['mean_threshold'] + std*self.cfg['std_threshold']
        #self.debug("mean=%f, std=%f, threshold=%f, min_threshold=%d" % (mean, std, lat_threshold, min_threshold))
        if lat_threshold < min_threshold:
            lat_threshold = min_threshold

        new_weights = dict([(osd, 1.0) for osd in lats.keys()])
        for osd,v in lats.items():
            if v > lat_threshold:
                # round to reweight_step
                w = mean*1.0 / v
                w = int(w / self.cfg['reweight_step'])
                w = self.cfg['reweight_step'] * w
                new_weights[osd] = min(new_weights[osd], w)
        #self.debug(new_weights, "new weights")

        return new_weights


    def process_osd_lats(self, lats, min_threshold):
        if len(lats[0]) < 3 or len(lats[1]) < 3 or len(lats[0]) != len(lats[1]):
            return {}
        apply_new_weights = self.get_new_weights(lats[0], min_threshold)
        commit_new_weights = self.get_new_weights(lats[1], min_threshold)
        # get min weight according apply or commit latency
        return dict([(osd, min(apply_new_weights[osd], commit_new_weights[osd])) for osd in apply_new_weights.keys()])


    def collect_stats(self):
        self.osd_map_crush = self.get("osd_map_crush")
        self.osd_stats = self.get("osd_stats")

        if 'devices' not in self.osd_map_crush or len(self.osd_map_crush['devices']) == 0:
            return False
        if 'osd_stats' not in self.osd_stats or len(self.osd_stats['osd_stats']) == 0:
            return False

        try:
            self.osd_apply_lats = dict([(o['osd'], o['perf_stat']['apply_latency_ms']) for o in self.osd_stats['osd_stats']
                                        if 'osd' in o and 'perf_stat' in o and 'apply_latency_ms' in o['perf_stat']])
            self.osd_commit_lats = dict([(o['osd'], o['perf_stat']['commit_latency_ms']) for o in self.osd_stats['osd_stats']
                                        if 'osd' in o and 'perf_stat' in o and 'commit_latency_ms' in o['perf_stat']])
            #self.debug({'apply': self.osd_apply_lats, 'commit': self.osd_commit_lats})
        except Exception as e:
            self.debug(e)
            return False

        if self.cfg['dump_latency']:
            with open(self.cfg['dump_latency_filename'], "a+") as dumpfile:
                dumpfile.write(json.dumps({"apply":self.osd_apply_lats, "commit":self.osd_commit_lats}))
                dumpfile.write("\n")

        return True


    def aggregate_lats(self):
        self.osd_class = {}
        try:
            self.osd_class = dict([(o['id'], o['class']) for o in self.osd_map_crush['devices']
                                   if 'class' in o and 'id' in o])
            #self.debug(self.osd_class, 'device class')
        except Exception as e:
            self.debug(e)
            return False

        # update latency window
        # {class -> [{osd_id -> [apply_lats]}, {osd_id -> [commit_lats]}]}
        for osd,c in self.osd_class.items():
            if osd not in self.osd_apply_lats or osd not in self.osd_commit_lats:
                continue
            if c not in self.window:
                self.window[c] = [{}, {}]
            if osd not in self.window[c][0]:
                self.window[c][0][osd] = []
            if osd not in self.window[c][1]:
                self.window[c][1][osd] = []
            self.window[c][0][osd].append(self.osd_apply_lats[osd])
            self.window[c][1][osd].append(self.osd_commit_lats[osd])
            if len(self.window[c][0][osd]) > self.cfg['window_width']:
                self.window[c][0][osd] = self.window[c][0][osd][-self.cfg['window_width']:]
            if len(self.window[c][1][osd]) > self.cfg['window_width']:
                self.window[c][1][osd] = self.window[c][1][osd][-self.cfg['window_width']:]

        # cleanup window from removed osds
        for c,v in self.window.items():
            to_remove = []
            for osd in self.window[c][0]:
                if osd not in self.osd_apply_lats:
                    to_remove.append(osd)
            for osd in to_remove:
                del self.window[c][0][osd]
                del self.window[c][1][osd]
        self.debug(self.window, "window")

        # calculate average latencies
        # {class -> [{osd_id -> avg_apply_lat}, {osd_id -> avg_commit_lat}]}
        self.avg_lats = {}
        for osd,c in self.osd_class.items():
            if c not in self.avg_lats:
                self.avg_lats[c] = [{}, {}]
            # prevent reweights before collected stats, for example on startup of plugin or osd
            if osd not in self.window[c][0] or osd not in self.window[c][1]:
                continue
            if len(self.window[c][0][osd]) == self.cfg['window_width'] and len(self.window[c][1][osd]) == self.cfg['window_width']:
                self.avg_lats[c][0][osd] = float(sum(self.window[c][0][osd])) / len(self.window[c][0][osd])
                self.avg_lats[c][1][osd] = float(sum(self.window[c][1][osd])) / len(self.window[c][1][osd])
        #self.debug(self.avg_lats, "average latency")
        return True


    def recalculate_throttling(self):
        if not self.aggregate_lats():
            return False

        self.osd_map = self.get("osd_map")
        
        if 'osds' not in self.osd_map or len(self.osd_map['osds']) == 0:
            return False

        try:
            self.osd_state = dict([(o['osd'], {'up':o['up'], 'in':o['in']}) for o in self.osd_map['osds']])
            #self.debug(self.osd_state)
            weights = dict([(o['osd'], float(o['weight'])) for o in self.osd_map['osds']])
            #self.debug(weights)
        except Exception as e:
            self.debug(e)
            return False

        # update cached weights
        for osd,w in weights.items():
            if osd not in self.weights:
                self.weights[osd] = [w, True]
            else:
                self.weights[osd][0] = w

        # cleanup weights of removed osds
        to_remove = []
        for osd in self.weights:
            if osd not in weights:
                to_remove.append(osd)
        for osd in to_remove:
            del self.weights[osd]

        # filter only up and in osds
        # {class -> [{osd_id -> avg_apply_lat}, {osd_id -> avg_commit_lat}]}
        lats = {}
        for c,v in self.avg_lats.items():
            lats[c] = [{}, {}]
            for osd,lat in v[0].items():
                if self.osd_state[osd]['in'] == 1 and self.osd_state[osd]['up'] == 1:
                    lats[c][0][osd] = lat
            for osd,lat in v[1].items():
                if self.osd_state[osd]['in'] == 1 and self.osd_state[osd]['up'] == 1:
                    lats[c][1][osd] = lat
        self.debug(lats, 'filtered average latencies')

        # calculate new weights with min_latency according to osd class
        target_weights = {}

        for c,v in lats.items():
            min_threshold = {
                "ssd" : self.cfg['max_compliant_latency_ssd'],
                "hdd" : self.cfg['max_compliant_latency_hdd'],
            }.get(c, self.cfg['max_compliant_latency'])
            target_weights[c] = self.process_osd_lats(v, min_threshold)

        self.target_weights = {}
        for c,v in target_weights.items():
            self.target_weights.update(v)

        return True


    def apply_throttling(self):
        self.debug(self.target_weights, "target weights")

        self.pg_summary = self.get("pg_summary")

        if 'all' not in self.pg_summary:
            return

        # do reweight only if cluster have all pgs active+clean
        self.reweight_allowed = len(self.pg_summary['all']) == 1 and 'active+clean' in self.pg_summary['all']
        if self.reweight_allowed:

            # decide do we have rights to reweight this osd
            # start and stop reweight only if weight=1.0
            max_throttled_osds = int(len(self.weights)*self.cfg['max_throttled_osds'])
            throttled_osds = 0

            for osd,w in self.weights.items():
                if w[0] == 1.0:
                    self.weights[osd][1] = True
                elif not w[1]:
                    throttled_osds += 1

            for osd,w in sorted(self.target_weights.items(), key=operator.itemgetter(1)):
                if throttled_osds < max_throttled_osds and self.weights[osd] and w < 1.0:
                    self.weights[osd][1] = False
                    throttled_osds += 1

            # do actual reweight
            for osd,w in self.target_weights.items():
                if not self.weights[osd][1]:
                    k = 1.0 - self.cfg['reweight_step']
                    if (w > self.weights[osd][0] + self.cfg['reweight_step']):
                        w = self.weights[osd][0] / k
                        if w > 1.0:
                            w = 1.0
                    elif w < (self.weights[osd][0] - self.cfg['reweight_step']):
                        w = self.weights[osd][0] * k
                    if w != self.weights[osd][0]:
                        self.debug("reweight osd=%d weight=%f" % (osd, w))
                        self.do_reweight_sync(osd, w)
                        self.weights[osd][0] = w
        self.debug(self.weights, "weights")


    def ld_cfg(self, name, type=str, default=""):
        self.cfg[name] = type(self.get_localized_config(name, default))


    def load_config(self):
        self.ld_cfg('period', int, DEFAULT_PERIOD)
        self.ld_cfg('max_compliant_latency', float, DEFAULT_MAX_COMPLIANT_LATENCY)
        self.ld_cfg('max_compliant_latency_ssd', float, DEFAULT_MAX_COMPLIANT_LATENCY_SSD)
        self.ld_cfg('max_compliant_latency_hdd', float, DEFAULT_MAX_COMPLIANT_LATENCY_HDD)
        self.ld_cfg('latency_limit_mult', float, DEFAULT_LATENCY_LIMIT_MULT)
        self.ld_cfg('window_width', int, DEFAULT_WINDOW)
        self.ld_cfg('enable_debug', bool, DEFAULT_ENABLE_DEBUG)
        self.ld_cfg('dry_run', bool, DEFAULT_DRY_RUN)
        self.ld_cfg('max_throttled_osds', float, DEFAULT_MAX_THROTTLED_OSDS)
        self.ld_cfg('dump_latency', bool, DEFAULT_DUMP_LATENCY)
        self.ld_cfg('dump_latency_filename', str, DEFAULT_DUMP_LATENCY_FILENAME)
        self.debug(self.cfg, 'config')


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
            self.debug_start()
            self.load_config()
            try:
                if self.collect_stats():
                    if self.recalculate_throttling() and not self.cfg['dry_run']:
                        self.apply_throttling()
            except Exception as e:
                self.debug(e)
            self.elapsed = datetime.datetime.now() - self.tick
            self.debug(self.elapsed, "elapsed")


    def shutdown(self):
        cherrypy.engine.exit()
        self.serving = False
        self.worker.join()
        pass


    def serve(self):

        #TODO: expose module state in json
        class Root(object):

            # collapse everything to '/'
            def _cp_dispatch(self, vpath):
                cherrypy.request.path = ''
                return self

            @cherrypy.expose
            def index(self):
                cherrypy.response.headers['Content-Type'] = 'text/plain'
                return global_instance().debug_str

        self.load_config()

        self.serving = True
        self.worker = threading.Thread(target=lambda : self.process())
        self.worker.start()

        self.server_addr = self.get_localized_config('server_addr', DEFAULT_ADDR)
        self.server_port = self.get_localized_config('server_port', DEFAULT_PORT)
        self.log.info(
            "server_addr: %s, server_port: %s" %
            (self.server_addr, self.server_port)
        )

        cherrypy.config.update({
            'server.socket_host': self.server_addr,
            'server.socket_port': self.server_port,
            'engine.autoreload.on': False
        })
        cherrypy.tree.mount(Root(), "/")
        cherrypy.engine.start()
        cherrypy.engine.block()

