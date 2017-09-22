
"""
    Automatically reweight osds according to performance stats (latencies).
    =======================================================================

    Problem.
    --------

    osd:        3   5    8  1 2 7   6           9      0      4
    |-----------*---*----*--*-*-*---*-----------*------*------*----------> latency, ms
            10      20      30      40  ... 100 ... 300 ...  1500

    Principle overview.
    -------------------

    Feature list.
    -------------





"""

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
DEFAULT_DUMP_LATENCY = True
DEFAULT_DUMP_LATENCY_FILENAME = "/tmp/ceph_osd_latencies.json"

DEFAULT_PERIOD = 5 # process stats every 5 seconds
DEFAULT_WINDOW = 10 # width of window in periods
# if stats not contain osd then collected stats will be deleted
# so there is no need to control timestamps of stats values

# maximum absolute latency value when osd will not be reweighted
DEFAULT_MAX_COMPLIANT_LATENCY = 200
DEFAULT_MAX_COMPLIANT_LATENCY_SSD = 50
DEFAULT_MAX_COMPLIANT_LATENCY_HDD = 500

# latency threshold = mean*latency_mean_threshold + stddev*latency_std_threshold
DEFAULT_LATENCY_MEAN_THRESHOLD = 2.0
DEFAULT_LATENCY_STD_THRESHOLD = 5.0

# log distance threshold for selecting nearest points
DEFAULT_CLUSTERING_DISTANCE_THRESHOLD = 0.8

# reweight +/-this step then wait for backfill completion
DEFAULT_REWEIGHT_STEP = 0.05

# limit max number of throttled osds
DEFAULT_MAX_THROTTLED_OSDS = 0.2


_global_instance = {'plugin': None}

def global_instance():
    assert _global_instance['plugin'] is not None
    return _global_instance['plugin']

def get_mean_std(x):
    m = sum(x) / len(x)
    s = sum([(lat - m)*(lat - m) for lat in x])
    return m, math.sqrt(s / (len(x) - 1))

def get_log_threshold(values, log_threshold):
    a0 = min(values)
    a1 = max(values)
    a0 = a0 if a0 > 1 else 1
    a1 = a1 if a1 > 1 else 1
    A0 = math.log(a0)
    A1 = math.log(a1)
    X = (A1 - A0)*log_threshold + A0
    threshold = math.exp(X)
    global_instance().debug([(a0, a1), (A0, A1), X, threshold])
    return threshold

def get_nearest(points, log_threshold):
    '''
    For some osd dict of neighbours may be empty.
    :param x: dict(osd_id -> x) Points
    :return: dict(osd_id -> dict(osd_id -> distance)) Nearest points for every point.
    '''
    dist = {}
    s = []
    for id0,x0 in points.items():
        d0 = {}
        for id1,x1 in points.items():
            if id1 != id0:
                v = abs(x1 - x0)
                s.append(v)
                d0[id1] = v
        dist[id0] = d0
    threshold = get_log_threshold(s, log_threshold)
    result = {}
    for id0,d in dist.items():
        result[id0] = set([id for id,v in d.items() if v <= threshold])
    return result

def get_clusters(neighbours):
    '''
    Returns list of clusters.
    :param x: dict(osd_id -> dict(osd_id -> distance)) neighbours for each point
    :return: list(set(osd_id)) List of clusters
    '''
    # merge key with neighbours
    cells = [v.union(set([k])) for k,v in neighbours.items()]
    # merge cells until nothing can be merged
    clusters = []
    while len(cells) > 0:
        # for each cell A
        A = cells[0]
        cells = cells[1:]
        merged = True
        while merged:
            merged = False
            for i,B in enumerate(cells):
                # for each other cell B
                if len(A.intersection(B)) > 0:
                    # if A and B have commin elements
                    A = A.union(B)
                    del cells[i]
                    merged = True
                    break
        clusters.append(A)
    return clusters

class Module(MgrModule):

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.serving = False
        self.metrics = dict()
        self.debug_str = "started"
        self.weights = {}
        self.new_weights = {}
        self.elapsed = datetime.timedelta()
        self.window = {}
        self.enable_debug = DEFAULT_ENABLE_DEBUG
        _global_instance['plugin'] = self

    def do_reweight_sync(self, osd_id, weight):
        result = CommandResult("")
        self.send_command(result, "mon", "",
                           json.dumps({
                               "prefix": "osd reweight",
                               "id": osd_id,
                               "weight": weight,
                           }),
                           "")
        r, outb, outs = result.wait()

    def debug_start(self):
        if self.enable_debug:
            self.debug_str = ""
        else:
            self.debug_str = "disabled"

    def debug(self, obj, label=""):
        if self.enable_debug:
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
        neighbours = get_nearest(lats, self.distance_threshold)
        self.debug(neighbours, "neighbours")
        clusters = get_clusters(neighbours)
        self.debug(clusters, "clusters")
        dominant = max(clusters, key=len)
        self.debug(dominant, "dominant")

        dominant_lats = dict([(osd,lats[osd]) for osd in dominant])
        dominant_min = min(dominant_lats.values())
        # append all that lower for compliant stddev
        for osd,lat in lats.items():
            if lat < dominant_min and osd not in dominant_lats:
                dominant_lats[osd] = lat
        self.debug(dominant_lats, "dominant lats")

        mean, std = get_mean_std(dominant_lats.values())
        lat_threshold = mean*self.mean_threshold + std*self.std_threshold
        self.debug("mean=%f, std=%f, threshold=%f, min_threshold=%d" % (mean, std, lat_threshold, min_threshold))
        if lat_threshold < min_threshold:
            lat_threshold = min_threshold

        new_weights = dict([(osd, 1.0) for osd in lats.keys()])
        for osd,v in lats.items():
            if v > lat_threshold:
                # round to reweight_step
                w = mean*1.0 / v
                w = int(w / self.reweight_step)
                w = self.reweight_step * w
                new_weights[osd] = min(new_weights[osd], w)
        self.debug(new_weights, "new weights")

        return new_weights

    def process_osd_lats(self, lats, min_threshold):
        if len(lats[0]) < 3 or len(lats[1]) < 3 or len(lats[0]) != len(lats[1]):
            self.new_weights = {}
            return
        apply_new_weights = self.get_new_weights(lats[0], min_threshold)
        commit_new_weights = self.get_new_weights(lats[1], min_threshold)
        # get min weight according apply or commit latency
        self.new_weights = dict([(osd, min(apply_new_weights[osd], commit_new_weights[osd])) for osd in apply_new_weights.keys()])

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
            self.debug({'apply': self.osd_apply_lats, 'commit': self.osd_commit_lats})
        except Exception as e:
            self.debug(e)
            return False

        if self.dump_latency:
            with open(self.dump_latency_filename, "a+") as dumpfile:
                dumpfile.write(json.dumps({"apply":self.osd_apply_lats, "commit":self.osd_commit_lats}))
                dumpfile.write("\n")

    def aggregate_lats(self):
        self.osd_class = {}
        try:
            self.osd_class = dict([(o['id'], o['class']) for o in self.osd_map_crush['devices']
                                   if 'class' in o and 'id' in o])
            self.debug(self.osd_class)
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
            if len(self.window[c][0][osd]) > self.window_width:
                self.window[c][0][osd] = self.window[c][0][osd][-self.window_width:]
            if len(self.window[c][1][osd]) > self.window_width:
                self.window[c][1][osd] = self.window[c][1][osd][-self.window_width:]

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
            if len(self.window[c][0][osd]) == self.window_width and len(self.window[c][1][osd]) == self.window_width:
                self.avg_lats[c][0][osd] = float(sum(self.window[c][0][osd])) / len(self.window[c][0][osd])
                self.avg_lats[c][1][osd] = float(sum(self.window[c][1][osd])) / len(self.window[c][1][osd])
        self.debug(self.avg_lats, "average latency")
        return True

    def recalculate_weights(self):
        if not self.aggregate_lats():
            return False

        self.osd_map = self.get("osd_map")
        
        if 'osds' not in self.osd_map or len(self.osd_map['osds']) == 0:
            return False

        try:
            self.osd_state = dict([(o['osd'], {'up':o['up'], 'in':o['in']}) for o in self.osd_map['osds']])
            self.debug(self.osd_state)
            weights = dict([(o['osd'], float(o['weight'])) for o in self.osd_map['osds']])
            self.debug(weights)
        except Exception as e:
            self.debug(e)
            return False

        # update cached weights
        for osd,w in weights.items():
            if osd not in self.weights:
                self.weights[osd] = (w, True)
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
        for c,v in lats.items():
            min_threshold = {
                "ssd" : self.max_compliant_latency_ssd,
                "hdd" : self.max_compliant_latency_hdd,
            }.get(c, self.max_compliant_latency)
            self.process_osd_lats(v, min_threshold)
        return True

    #TODO: try regulator (PID, LQ, ..?), 1-st way: in-latency, out-weight + filter high weights, 2-nd way: in-weight, out-weight
    def apply_new_weights(self):
        self.pg_summary = self.get("pg_summary")

        if 'all' not in self.pg_summary:
            return

        # do reweight only if cluster have all pgs active+clean
        self.reweight_allowed = len(self.pg_summary['all']) == 1 and 'active+clean' in self.pg_summary['all']
        if self.reweight_allowed:
            self.debug(self.new_weights, "target weights")

            # decide do we have rights to reweight this osd
            # start and stop reweight only if weight=1.0
            max_throttled_osds = int(len(self.weights)*self.max_throttled_osds)
            throttled_osds = 0

            for osd,w in self.weights.items():
                if w[0] == 1.0:
                    self.weights[osd][1] = True
                elif not w[1]:
                    throttled_osds += 1

            for osd,w in sorted(self.new_weights.items(), key=operator.itemgetter(1)):
                if throttled_osds < max_throttled_osds and self.weights[osd] and w < 1.0:
                    self.weights[osd] = False
                    throttled_osds += 1

            # do actual reweight
            for osd,w in self.new_weights.items():
                if not self.weights[osd][1]:
                    k = 1.0 - self.reweight_step
                    if (w > self.weights[osd][0] + self.reweight_step):
                        w = self.weights[osd][0] / k
                        if w > 1.0:
                            w = 1.0
                    elif w < (self.weights[osd][0] - self.reweight_step):
                        w = self.weights[osd][0] * k
                    if w != self.weights[osd][0]:
                        self.debug("reweight osd=%d weight=%f" % (osd, w))
                        self.do_reweight_sync(osd, w)
                        self.weights[osd][0] = w
        self.debug(self.weights, "weights")

    def load_config(self):
        self.period = self.get_localized_config('period', DEFAULT_PERIOD)
        self.max_compliant_latency = self.get_localized_config('max_compliant_latency', DEFAULT_MAX_COMPLIANT_LATENCY)
        self.max_compliant_latency_ssd = self.get_localized_config('max_compliant_latency_ssd', DEFAULT_MAX_COMPLIANT_LATENCY_SSD)
        self.max_compliant_latency_hdd = self.get_localized_config('max_compliant_latency_hdd', DEFAULT_MAX_COMPLIANT_LATENCY_HDD)
        self.std_threshold = self.get_localized_config('std_threshold', DEFAULT_LATENCY_STD_THRESHOLD)
        self.mean_threshold = self.get_localized_config('mean_threshold', DEFAULT_LATENCY_MEAN_THRESHOLD)
        self.distance_threshold = self.get_localized_config('distance_threshold', DEFAULT_CLUSTERING_DISTANCE_THRESHOLD)
        self.reweight_step = self.get_localized_config('reweight_step', DEFAULT_REWEIGHT_STEP)
        self.window_width = self.get_localized_config('window', DEFAULT_WINDOW)
        self.enable_debug = self.get_localized_config('enable_debug', DEFAULT_ENABLE_DEBUG)
        self.dry_run = self.get_localized_config('dry_run', DEFAULT_DRY_RUN)
        self.max_throttled_osds = self.get_localized_config('max_throttled_osds', DEFAULT_MAX_THROTTLED_OSDS)
        self.dump_latency = self.get_localized_config('dump_lats', DEFAULT_DUMP_LATENCY)
        self.dump_latency_filename = self.get_localized_config('dump_latency_filename', DEFAULT_DUMP_LATENCY_FILENAME)

    def process(self):
        self.log.info("start process osds")
        self.tick = datetime.datetime.now()
        while self.serving:
            next_tick = self.tick + datetime.timedelta(seconds=self.period)
            while datetime.datetime.now() < next_tick and self.serving:
                time.sleep(0.1)
            if not self.serving:
                break
            self.tick = datetime.datetime.now()
            self.debug_start()
            self.load_config()
            #TODO: collect cluster state
            #TODO: if latency goes down (to 0 for example) weight will be restored - twice period holding weight down before rising every such period
            #TODO: select osds need to reweight: k_nearest, log_distance, k_nearest+stddev, median_nearest+stddev, log_distance+stddev, 1d_gmm_mle
            #TODO: calc theoretical weights
            #TODO: do reweight with constraints
            try:
                if self.collect_stats():
                    if self.recalculate_weights() and len(self.new_weights) > 0 and not self.dry_run:
                        self.apply_new_weights()
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

