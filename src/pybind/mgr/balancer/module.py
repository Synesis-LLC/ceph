
"""
Balance PG distribution across OSDs.
"""

import copy
import errno
import json
import math
import random
import time
from mgr_module import MgrModule, CommandResult
from module_cfg import ModuleConfig
from threading import Event
from pprint import pformat
import numpy as np
import sys

# available modes: 'none', 'crush', 'crush-compat', 'upmap', 'osd_weight'
DEFAULT_MODE = 'reweight'
DEFAULT_SLEEP_INTERVAL = 60   # seconds
DEFAULT_MAX_MISPLACED = 0.0    # max ratio of pgs replaced at a time

TIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

class MappingState:
    def __init__(self, osdmap, pg_dump, desc=''):
        self.desc = desc
        self.osdmap = osdmap
        self.osdmap_dump = self.osdmap.dump()
        self.crush = osdmap.get_crush()
        self.crush_dump = self.crush.dump()
        self.pg_dump = pg_dump
        self.pg_stat = {
            i['pgid']: i['stat_sum'] for i in pg_dump.get('pg_stats', [])
        }
        self.poolids = [p['pool'] for p in self.osdmap_dump.get('pools', [])]
        self.pg_up = {}
        self.pg_up_by_poolid = {}
        for poolid in self.poolids:
            self.pg_up_by_poolid[poolid] = osdmap.map_pool_pgs_up(poolid)
            for a,b in self.pg_up_by_poolid[poolid].iteritems():
                self.pg_up[a] = b

    def calc_misplaced_from(self, other_ms):
        num = len(other_ms.pg_up)
        misplaced = 0
        for pgid, before in other_ms.pg_up.iteritems():
            if before != self.pg_up.get(pgid, []):
                misplaced += 1
        if num > 0:
            return float(misplaced) / float(num)
        return 0.0

    def dump(self, path=None):
        return {
            'desc' : self.desc,
            'osdmap_dump' : self.osdmap_dump,
            'crush_dump' : self.crush_dump,
            'pg_dump' : self.pg_dump,
            'pg_stat' : self.pg_stat,
            'poolids' : self.poolids,
            'pg_up' : self.pg_up,
            'pg_up_by_poolid' : self.pg_up_by_poolid
        }


class Plan:
    def __init__(self, name, ms):
        self.mode = 'unknown'
        self.name = name
        self.initial = ms

        self.osd_weights = {}
        self.compat_ws = {}
        self.inc = ms.osdmap.new_incremental()
        self.osd_by_device_class = {}

    def final_state(self):
        self.inc.set_osd_reweights(self.osd_weights)
        self.inc.set_crush_compat_weight_set_weights(self.compat_ws)
        return MappingState(self.initial.osdmap.apply_incremental(self.inc),
                            self.initial.pg_dump,
                            'plan %s final' % self.name)

    def dump(self, path=None):
        all = {
            'mode' : self.mode,
            'name' : self.name,
            'osd_by_device_class' : self.osd_by_device_class,
            'osd_weights' : self.osd_weights,
            'compat_ws' : self.compat_ws,

            'initial' : self.initial.dump(),
            'incremental' : self.inc.dump()
        }
        if path:
            subtree = all
            for el in path.split('.'):
                if el in subtree:
                    subtree = subtree[el]
                else:
                    if el.isdigit():
                        el = int(el)
                    if el in subtree:
                        subtree = subtree[el]
                    else:
                        return {'message': "Can't find path",
                                'path': path.split('.')}
            return subtree
        else:
            return all

    def dump_str(self, path=None):
        return json.dumps(self.dump(path), indent=4)

    def show(self):
        ls = []
        ls.append('# starting osdmap epoch %d' % self.initial.osdmap.get_epoch())
        ls.append('# starting crush version %d' %
                  self.initial.osdmap.get_crush_version())
        ls.append('# mode %s' % self.mode)
        if len(self.compat_ws) and \
           '-1' not in self.initial.crush_dump.get('choose_args', {}):
            ls.append('ceph osd crush weight-set create-compat')
        for osd, weight in self.compat_ws.iteritems():
            ls.append('ceph osd crush weight-set reweight-compat %s %f' %
                      (osd, weight))
        for osd, weight in self.osd_weights.iteritems():
            ls.append('ceph osd reweight osd.%d %f' % (osd, weight))
        incdump = self.inc.dump()
        for pgid in incdump.get('old_pg_upmap_items', []):
            ls.append('ceph osd rm-pg-upmap-items %s' % pgid)
        for item in incdump.get('new_pg_upmap_items', []):
            osdlist = []
            for m in item['mappings']:
                osdlist += [m['from'], m['to']]
            ls.append('ceph osd pg-upmap-items %s %s' %
                      (item['pgid'], ' '.join([str(a) for a in osdlist])))
        return '\n'.join(ls)


class Eval:
    root_ids = {}        # root name -> id
    pool_name = {}       # pool id -> pool name
    pool_id = {}         # pool name -> id
    pool_roots = {}      # pool name -> root name
    root_pools = {}      # root name -> pools
    target_by_root = {}  # root name -> target weight map
    count_by_pool = {}
    count_by_root = {}
    actual_by_pool = {}  # pool -> by_* -> actual weight map
    actual_by_root = {}  # pool -> by_* -> actual weight map
    total_by_pool = {}   # pool -> by_* -> total
    total_by_root = {}   # root -> by_* -> total
    stats_by_pool = {}   # pool -> by_* -> stddev or avg -> value
    stats_by_root = {}   # root -> by_* -> stddev or avg -> value

    score_by_pool = {}
    score_by_root = {}

    score = 0.0

    def __init__(self, ms):
        self.ms = ms

    def show(self, verbose=False):
        if verbose:
            r = self.ms.desc + '\n'
            r += 'target_by_root %s\n' % self.target_by_root
            r += 'actual_by_pool %s\n' % self.actual_by_pool
            r += 'actual_by_root %s\n' % self.actual_by_root
            r += 'count_by_pool %s\n' % self.count_by_pool
            r += 'count_by_root %s\n' % self.count_by_root
            r += 'total_by_pool %s\n' % self.total_by_pool
            r += 'total_by_root %s\n' % self.total_by_root
            r += 'stats_by_root %s\n' % self.stats_by_root
            r += 'score_by_pool %s\n' % self.score_by_pool
            r += 'score_by_root %s\n' % self.score_by_root
        else:
            r = self.ms.desc + ' '
        r += 'score %f (lower is better)\n' % self.score
        return r

    def calc_stats(self, count, target, total):
        num = max(len(target), 1)
        r = {}
        for t in ('pgs', 'objects', 'bytes'):
            avg = float(total[t]) / float(num)
            dev = 0.0

            # score is a measure of how uneven the data distribution is.
            # score lies between [0, 1), 0 means perfect distribution.
            score = 0.0
            sum_weight = 0.0

            for k, v in count[t].iteritems():
                # adjust/normalize by weight
                if target[k]:
                    adjusted = float(v) / target[k] / float(num)
                else:
                    adjusted = 0.0

                # Overweighted devices and their weights are factors to calculate reweight_urgency.
                # One 10% underfilled device with 5 2% overfilled devices, is arguably a better
                # situation than one 10% overfilled with 5 2% underfilled devices
                if adjusted > avg:
                    '''
                    F(x) = 2*phi(x) - 1, where phi(x) = cdf of standard normal distribution
                    x = (adjusted - avg)/avg.
                    Since, we're considering only over-weighted devices, x >= 0, and so phi(x) lies in [0.5, 1).
                    To bring range of F(x) in range [0, 1), we need to make the above modification.

                    In general, we need to use a function F(x), where x = (adjusted - avg)/avg
                    1. which is bounded between 0 and 1, so that ultimately reweight_urgency will also be bounded.
                    2. A larger value of x, should imply more urgency to reweight.
                    3. Also, the difference between F(x) when x is large, should be minimal.
                    4. The value of F(x) should get close to 1 (highest urgency to reweight) with steeply.

                    Could have used F(x) = (1 - e^(-x)). But that had slower convergence to 1, compared to the one currently in use.

                    cdf of standard normal distribution: https://stackoverflow.com/a/29273201
                    '''
                    score += target[k] * (math.erf(((adjusted - avg)/avg) / math.sqrt(2.0)))
                    sum_weight += target[k]
                dev += (avg - adjusted) * (avg - adjusted)
            stddev = math.sqrt(dev / float(max(num - 1, 1)))
            score = score / max(sum_weight, 1)
            r[t] = {
                'avg': avg,
                'stddev': stddev,
                'sum_weight': sum_weight,
                'score': score,
            }
        return r

class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "balancer status",
            "desc": "Show balancer status",
            "perm": "r",
        },
        {
            "cmd": "balancer mode name=mode,type=CephChoices,strings=none|crush-compat|upmap|reweight",
            "desc": "Set balancer mode",
            "perm": "rw",
        },
        {
            "cmd": "balancer on",
            "desc": "Enable automatic balancing",
            "perm": "rw",
        },
        {
            "cmd": "balancer off",
            "desc": "Disable automatic balancing",
            "perm": "rw",
        },
        {
            "cmd": "balancer eval name=plan,type=CephString,req=false",
            "desc": "Evaluate data distribution for the current cluster or specific plan",
            "perm": "r",
        },
        {
            "cmd": "balancer eval-verbose name=plan,type=CephString,req=false",
            "desc": "Evaluate data distribution for the current cluster or specific plan (verbosely)",
            "perm": "r",
        },
        {
            "cmd": "balancer optimize name=plan,type=CephString",
            "desc": "Run optimizer on plan",
            "perm": "rw",
        },
        {
            "cmd": "balancer show name=plan,type=CephString",
            "desc": "Show details of an optimization plan",
            "perm": "r",
        },
        {
            "cmd": "balancer rm name=plan,type=CephString",
            "desc": "Discard an optimization plan",
            "perm": "rw",
        },
        {
            "cmd": "balancer reset",
            "desc": "Discard all optimization plans",
            "perm": "rw",
        },
        {
            "cmd": "balancer dump name=plan,type=CephString, name=path,type=CephString,req=false",
            "desc": "Show an optimization plan",
            "perm": "r",
        },
        {
            "cmd": "balancer execute name=plan,type=CephString",
            "desc": "Execute an optimization plan",
            "perm": "r",
        },
        {
            "cmd": "balancer reweight name=device_class,type=CephString, name=new_weight,type=CephFloat",
            "desc": "Reweight all osds with device_class to new_weight",
            "perm": "rw",
        },
    ] + ModuleConfig.get_commands("balancer")
    active = False
    run = True
    plans = {}
    mode = ''

    config_options = {
        'mode' : (str, DEFAULT_MODE),
        'sleep_interval' : (int, DEFAULT_SLEEP_INTERVAL),
        'max_misplaced' : (float, DEFAULT_MAX_MISPLACED),
        'begin_time' : (str, '0000'),
        'end_time' : (str, '2400'),
        'upmap_max_iterations' : (int, 10),
        'upmap_max_deviation' : (float, .01),
        'crush_compat_max_iterations' : (int, 25),
        'crush_compat_step' : (float, .5),
        'active' : (bool, True),

        'stats_sanity_kb' : (int, 1),


        'hdd' : {
            'active' : (bool, True),
            # cv=stddev/mean
            'cv_max' : (float, .02),
            # do not reweight if avg usage is lower than
            'min_avg_usage' : (float, .05),
            # max step of weight change for each osd device class
            'max_reweight_step_inc' : (float, .02),
            'max_reweight_step_dec' : (float, .02),
            # will do reweight if even one osd weight difference greater than
            'max_usage_difference' : (float, .05),
            'abs_max_difference' : (bool, True),
            # max - pull max weight to 1.0
            # avg - set 0.5 weight for osd with avg usage
            # none - do not normalize
            'reweight_normalization_mode' : (str, 'max'),
            'reweight_normalization_avg_base' : (float, 0.5),
            # min usage_diff and usage_mult to reweight osd
            'min_osd_usage_diff' : (float, .005), # if avg_usage=0.5 and usage=0.51 than usage_diff=0.01
            'min_osd_usage_mult_diff' : (float, .01), # if avg_usage=0.5 and usage_diff=0.01 than usage_mult_diff=0.02
        },

        'ssd' : {
            'active' : (bool, True),
            'cv_max' : (float, .02),
            'min_avg_usage' : (float, .05),
            'max_reweight_step_inc' : (float, .02),
            'max_reweight_step_dec' : (float, .02),
            'max_usage_difference' : (float, .05),
            'abs_max_difference' : (bool, True),
            'reweight_normalization_mode' : (str, 'max'),
            'reweight_normalization_avg_base' : (float, 0.5),
            'min_osd_usage_diff' : (float, .005),
            'min_osd_usage_mult_diff' : (float, .01),
        },
    }

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.cfg = ModuleConfig(self, self.config_options)
        self.event = Event()
        self.log.info('Python ' + sys.version)
        self.log.info('sys.path:')
        for path in sys.path:
            self.log.info('    ' + path)
        try:
            #self.log.info('try import dpath')
            #import dpath
            pass
        except Exception as e:
            self.log.error(e.message)


    def handle_command(self, command):
        self.log.warn("Handling command: '%s'" % str(command))

        handled, result = self.cfg.handle_command("balancer", command)
        if handled:
            return result

        if command['prefix'] == 'balancer status':
            s = {
                'plans': self.plans.keys(),
                'active': self.cfg.get('active'),
                'mode': self.cfg.get('mode'),
                'run' : self.run,
            }
            return (0, json.dumps(s, indent=4), '')

        elif command['prefix'] == 'balancer mode':
            self.cfg.set('mode', command['mode'])
            return (0, '', '')

        elif command['prefix'] == 'balancer on':
            self.cfg.enable('active')
            self.event.set()
            return (0, '', '')

        elif command['prefix'] == 'balancer off':
            self.cfg.disable('active')
            self.event.set()
            return (0, '', '')

        elif command['prefix'] == 'balancer eval' or command['prefix'] == 'balancer eval-verbose':
            verbose = command['prefix'] == 'balancer eval-verbose'
            if 'plan' in command:
                plan = self.plans.get(command['plan'])
                if not plan:
                    return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
                ms = plan.final_state()
            else:
                ms = MappingState(self.get_osdmap(),
                                  self.get("pg_dump"),
                                  'current cluster')
            return (0, self.evaluate(ms, verbose=verbose), '')

        elif command['prefix'] == 'balancer optimize':
            self.log.error('plan_create %s' % command['plan'])
            plan = self.plan_create(command['plan'])
            self.log.error('optimize %s' % command['plan'])
            self.optimize(plan)
            self.log.error('optimization finished %s' % command['plan'])
            return (0, '', '')

        elif command['prefix'] == 'balancer rm':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            self.plan_rm(command['plan'])
            return (0, '', '')

        elif command['prefix'] == 'balancer reset':
            self.plans = {}
            return (0, '', '')

        elif command['prefix'] == 'balancer dump':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            if 'path' in command:
                return (0, plan.dump_str(command['path']), '')
            else:
                return (0, plan.dump_str(), '')

        elif command['prefix'] == 'balancer show':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            return (0, plan.show(), '')

        elif command['prefix'] == 'balancer execute':
            plan = self.plans.get(command['plan'])
            if not plan:
                return (-errno.ENOENT, '', 'plan %s not found' % command['plan'])
            self.execute(plan)
            self.plan_rm(command['plan'])
            return (0, '', '')

        elif command['prefix'] == 'balancer reweight':
            device_class = command['device_class']
            new_weight = command['new_weight']
            result = self.reweight(device_class, new_weight)
            if result[0] != 0:
                return (result[0], '', result[1])
            return (0, '', '')

        else:
            return (-errno.EINVAL, '', "Command not found '{0}'".format(command['prefix']))

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def time_in_interval(self, tod, begin, end):
        if begin <= end:
            return tod >= begin and tod < end
        else:
            return tod >= begin or tod < end

    def serve(self):
        self.cfg.load()
        self.log.error('Starting')
        while self.run:
            begin_time = self.cfg.get('begin_time')
            end_time = self.cfg.get('end_time')
            timeofday = time.strftime('%H%M', time.localtime())
            self.log.error('Waking up [%s, scheduled for %s-%s, now %s]',
                           "active" if self.cfg.get('active') else "inactive",
                           begin_time, end_time, timeofday)
            sleep_interval = float(self.cfg.get('sleep_interval'))
            if self.cfg.get('active') and self.time_in_interval(timeofday, begin_time, end_time):
                self.log.error('Running')
                name = 'auto_%s' % time.strftime(TIME_FORMAT, time.gmtime())
                plan = self.plan_create(name)
                if self.optimize(plan):
                    self.execute(plan)
                self.plan_rm(name)
            self.log.error('Sleeping for %d', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def reweight(self, device_class, new_weight):
        osdmap = self.get_osdmap()
        osdmap_dump = osdmap.dump()
        crush = osdmap.get_crush()
        crush_dump = crush.dump()
        osds = {osd['osd']:osd for osd in osdmap_dump.get('osds',[])}
        devices = crush_dump['devices']
        osd_by_device_class = {}
        device_class = str(device_class)
        new_weight = float(new_weight)

        if new_weight > 1.0 or new_weight < 0.0:
            return (-errno.EINVAL, 'Invalid weight %f' % new_weight)

        for device in devices:
            osd_id = device['id']
            if osd_id not in osds:
                self.log.error('No osd with id: %d' % osd_id)
                continue
            osd = osds[osd_id]
            if 'class' not in device:
                self.log.error('No osd class: ' + json.dumps(device))
                continue
            if osd['in'] != 1 or osd['up'] != 1:
                self.log.error('skip osd.%d in=%d up=%d' % (osd_id, osd['in'], osd['up']))
                continue
            if device['class'] not in osd_by_device_class:
                osd_by_device_class[device['class']] = set()
            osd_by_device_class[device['class']].add(device['id'])

        if device_class not in osd_by_device_class:
            return (-errno.EINVAL, 'Not found osd with device class "%s"' % device_class)

        reweightn = {}
        for osd in osd_by_device_class[device_class]:
            reweightn[str(osd)] = str(int(new_weight * float(0x10000)))

        commands = []
        if len(reweightn):
            self.log.error('ceph osd reweightn %s', reweightn)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd reweightn',
                'format': 'json',
                'weights': json.dumps(reweightn),
            }), 'foo')
            commands.append(result)

        self.log.error('commands %s' % commands)
        for result in commands:
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error on command')
                return
        self.log.error('done')

        return (0, '')

    def plan_create(self, name):
        plan = Plan(name, MappingState(self.get_osdmap(),
                                       self.get("pg_dump"),
                                       'plan %s initial' % name))
        self.plans[name] = plan
        return plan

    def plan_rm(self, name):
        if name in self.plans:
            del self.plans[name]

    def calc_eval(self, ms):
        pe = Eval(ms)
        pool_rule = {}
        pool_info = {}
        for p in ms.osdmap_dump.get('pools',[]):
            pe.pool_name[p['pool']] = p['pool_name']
            pe.pool_id[p['pool_name']] = p['pool']
            pool_rule[p['pool_name']] = p['crush_rule']
            pe.pool_roots[p['pool_name']] = []
            pool_info[p['pool_name']] = p
        pools = pe.pool_id.keys()
        if len(pools) == 0:
            return pe
        self.log.debug('pool_name %s' % pe.pool_name)
        self.log.debug('pool_id %s' % pe.pool_id)
        self.log.debug('pools %s' % pools)
        self.log.debug('pool_rule %s' % pool_rule)

        osd_weight = { a['osd']: a['weight']
                       for a in ms.osdmap_dump.get('osds',[]) }

        # get expected distributions by root
        actual_by_root = {}
        rootids = ms.crush.find_takes()
        roots = []
        for rootid in rootids:
            root = ms.crush.get_item_name(rootid)
            pe.root_ids[root] = rootid
            roots.append(root)
            ls = ms.osdmap.get_pools_by_take(rootid)
            pe.root_pools[root] = []
            for poolid in ls:
                pe.pool_roots[pe.pool_name[poolid]].append(root)
                pe.root_pools[root].append(pe.pool_name[poolid])
            weight_map = ms.crush.get_take_weight_osd_map(rootid)
            adjusted_map = {
                osd: cw * osd_weight.get(osd, 1.0)
                for osd,cw in weight_map.iteritems()
            }
            sum_w = sum(adjusted_map.values()) or 1.0
            pe.target_by_root[root] = { osd: w / sum_w
                                        for osd,w in adjusted_map.iteritems() }
            actual_by_root[root] = {
                'pgs': {},
                'objects': {},
                'bytes': {},
            }
            for osd in pe.target_by_root[root].iterkeys():
                actual_by_root[root]['pgs'][osd] = 0
                actual_by_root[root]['objects'][osd] = 0
                actual_by_root[root]['bytes'][osd] = 0
            pe.total_by_root[root] = {
                'pgs': 0,
                'objects': 0,
                'bytes': 0,
            }
        self.log.debug('pool_roots %s' % pe.pool_roots)
        self.log.debug('root_pools %s' % pe.root_pools)
        self.log.debug('target_by_root %s' % pe.target_by_root)

        # pool and root actual
        for pool, pi in pool_info.iteritems():
            poolid = pi['pool']
            pm = ms.pg_up_by_poolid[poolid]
            pgs = 0
            objects = 0
            bytes = 0
            pgs_by_osd = {}
            objects_by_osd = {}
            bytes_by_osd = {}
            for root in pe.pool_roots[pool]:
                for osd in pe.target_by_root[root].iterkeys():
                    pgs_by_osd[osd] = 0
                    objects_by_osd[osd] = 0
                    bytes_by_osd[osd] = 0
            for pgid, up in pm.iteritems():
                for osd in [int(osd) for osd in up]:
                    pgs_by_osd[osd] += 1
                    objects_by_osd[osd] += ms.pg_stat[pgid]['num_objects']
                    bytes_by_osd[osd] += ms.pg_stat[pgid]['num_bytes']
                    # pick a root to associate this pg instance with.
                    # note that this is imprecise if the roots have
                    # overlapping children.
                    # FIXME: divide bytes by k for EC pools.
                    for root in pe.pool_roots[pool]:
                        if osd in pe.target_by_root[root]:
                            actual_by_root[root]['pgs'][osd] += 1
                            actual_by_root[root]['objects'][osd] += ms.pg_stat[pgid]['num_objects']
                            actual_by_root[root]['bytes'][osd] += ms.pg_stat[pgid]['num_bytes']
                            pgs += 1
                            objects += ms.pg_stat[pgid]['num_objects']
                            bytes += ms.pg_stat[pgid]['num_bytes']
                            pe.total_by_root[root]['pgs'] += 1
                            pe.total_by_root[root]['objects'] += ms.pg_stat[pgid]['num_objects']
                            pe.total_by_root[root]['bytes'] += ms.pg_stat[pgid]['num_bytes']
                            break
            pe.count_by_pool[pool] = {
                'pgs': {
                    k: v
                    for k, v in pgs_by_osd.iteritems()
                },
                'objects': {
                    k: v
                    for k, v in objects_by_osd.iteritems()
                },
                'bytes': {
                    k: v
                    for k, v in bytes_by_osd.iteritems()
                },
            }
            pe.actual_by_pool[pool] = {
                'pgs': {
                    k: float(v) / float(max(pgs, 1))
                    for k, v in pgs_by_osd.iteritems()
                },
                'objects': {
                    k: float(v) / float(max(objects, 1))
                    for k, v in objects_by_osd.iteritems()
                },
                'bytes': {
                    k: float(v) / float(max(bytes, 1))
                    for k, v in bytes_by_osd.iteritems()
                },
            }
            pe.total_by_pool[pool] = {
                'pgs': pgs,
                'objects': objects,
                'bytes': bytes,
            }
        for root, m in pe.total_by_root.iteritems():
            pe.count_by_root[root] = {
                'pgs': {
                    k: float(v)
                    for k, v in actual_by_root[root]['pgs'].iteritems()
                },
                'objects': {
                    k: float(v)
                    for k, v in actual_by_root[root]['objects'].iteritems()
                },
                'bytes': {
                    k: float(v)
                    for k, v in actual_by_root[root]['bytes'].iteritems()
                },
            }
            pe.actual_by_root[root] = {
                'pgs': {
                    k: float(v) / float(max(pe.total_by_root[root]['pgs'], 1))
                    for k, v in actual_by_root[root]['pgs'].iteritems()
                },
                'objects': {
                    k: float(v) / float(max(pe.total_by_root[root]['objects'], 1))
                    for k, v in actual_by_root[root]['objects'].iteritems()
                },
                'bytes': {
                    k: float(v) / float(max(pe.total_by_root[root]['bytes'], 1))
                    for k, v in actual_by_root[root]['bytes'].iteritems()
                },
            }
        self.log.debug('actual_by_pool %s' % pe.actual_by_pool)
        self.log.debug('actual_by_root %s' % pe.actual_by_root)

        # average and stddev and score
        pe.stats_by_root = {
            a: pe.calc_stats(
                b,
                pe.target_by_root[a],
                pe.total_by_root[a]
            ) for a, b in pe.count_by_root.iteritems()
        }

	# the scores are already normalized
        pe.score_by_root = {
            r: {
                'pgs': pe.stats_by_root[r]['pgs']['score'],
                'objects': pe.stats_by_root[r]['objects']['score'],
                'bytes': pe.stats_by_root[r]['bytes']['score'],
            } for r in pe.total_by_root.keys()
        }

        # total score is just average of normalized stddevs
        pe.score = 0.0
        for r, vs in pe.score_by_root.iteritems():
            for k, v in vs.iteritems():
                pe.score += v
        pe.score /= 3 * len(roots)
        return pe

    def evaluate(self, ms, verbose=False):
        pe = self.calc_eval(ms)
        return pe.show(verbose=verbose)

    def optimize(self, plan):
        self.log.error('Optimize plan %s' % plan.name)
        plan.mode = self.cfg.get('mode')
        max_misplaced = self.cfg.get('max_misplaced')
        self.log.error('Mode %s, max misplaced %f' %
                      (plan.mode, max_misplaced))

        info = self.get('pg_status')
        unknown = info.get('unknown_pgs_ratio', 0.0)
        degraded = info.get('degraded_ratio', 0.0)
        inactive = info.get('inactive_pgs_ratio', 0.0)
        misplaced = info.get('misplaced_ratio', 0.0)
        self.log.error('unknown %f degraded %f inactive %f misplaced %g',
                       unknown, degraded, inactive, misplaced)
        if unknown > 0.0:
            self.log.error('Some PGs (%f) are unknown; waiting', unknown)
        elif degraded > 0.0:
            self.log.error('Some objects (%f) are degraded; waiting', degraded)
        elif inactive > 0.0:
            self.log.error('Some PGs (%f) are inactive; waiting', inactive)
        elif misplaced > max_misplaced:
            self.log.error('Too many objects (%f > %f) are misplaced; waiting',
                          misplaced, max_misplaced)
        else:
            if plan.mode == 'upmap':
                return self.do_upmap(plan)
            elif plan.mode == 'crush-compat':
                return self.do_crush_compat(plan)
            elif plan.mode == 'reweight':
                return self.optimize_reweight(plan)
            elif plan.mode == 'none':
                self.log.error('Idle')
            else:
                self.log.error('Unrecognized mode %s' % plan.mode)
        return False


    def do_upmap(self, plan):
        self.log.info('do_upmap')
        max_iterations = self.cfg.get('upmap_max_iterations')
        max_deviation = self.cfg.get('upmap_max_deviation')

        ms = plan.initial
        pools = [str(i['pool_name']) for i in ms.osdmap_dump.get('pools',[])]
        if len(pools) == 0:
            self.log.info('no pools, nothing to do')
            return False
        # shuffle pool list so they all get equal (in)attention
        random.shuffle(pools)
        self.log.info('pools %s' % pools)

        inc = plan.inc
        total_did = 0
        left = max_iterations
        for pool in pools:
            did = ms.osdmap.calc_pg_upmaps(inc, max_deviation, left, [pool])
            total_did += did
            left -= did
            if left <= 0:
                break
        self.log.info('prepared %d/%d changes' % (total_did, max_iterations))
        return True

    def do_crush_compat(self, plan):
        self.log.info('do_crush_compat')
        max_iterations = self.cfg.get('crush_compat_max_iterations')
        if max_iterations < 1:
            return False
        step = self.cfg.get('crush_compat_step')
        if step <= 0 or step >= 1.0:
            return False
        max_misplaced = self.cfg.get('max_misplaced')
        min_pg_per_osd = 2

        ms = plan.initial
        osdmap = ms.osdmap
        crush = osdmap.get_crush()
        pe = self.calc_eval(ms)
        if pe.score == 0:
            self.log.info('Distribution is already perfect')
            return False

        # get current osd reweights
        orig_osd_weight = { a['osd']: a['weight']
                            for a in ms.osdmap_dump.get('osds',[]) }
        reweighted_osds = [ a for a,b in orig_osd_weight.iteritems()
                            if b < 1.0 and b > 0.0 ]

        # get current compat weight-set weights
        orig_ws = self.get_compat_weight_set_weights()
        orig_ws = { a: b for a, b in orig_ws.iteritems() if a >= 0 }

        # Make sure roots don't overlap their devices.  If so, we
        # can't proceed.
        roots = pe.target_by_root.keys()
        self.log.debug('roots %s', roots)
        visited = {}
        overlap = {}
        root_ids = {}
        for root, wm in pe.target_by_root.iteritems():
            for osd in wm.iterkeys():
                if osd in visited:
                    overlap[osd] = 1
                visited[osd] = 1
        if len(overlap) > 0:
            self.log.err('error: some osds belong to multiple subtrees: %s' %
                         overlap)
            return False

        key = 'pgs'  # pgs objects or bytes

        # go
        best_ws = copy.deepcopy(orig_ws)
        best_ow = copy.deepcopy(orig_osd_weight)
        best_pe = pe
        left = max_iterations
        bad_steps = 0
        next_ws = copy.deepcopy(best_ws)
        next_ow = copy.deepcopy(best_ow)
        while left > 0:
            # adjust
            self.log.debug('best_ws %s' % best_ws)
            random.shuffle(roots)
            for root in roots:
                pools = best_pe.root_pools[root]
                pgs = len(best_pe.target_by_root[root])
                min_pgs = pgs * min_pg_per_osd
                if best_pe.total_by_root[root] < min_pgs:
                    self.log.info('Skipping root %s (pools %s), total pgs %d '
                                  '< minimum %d (%d per osd)',
                                  root, pools, pgs, min_pgs, min_pg_per_osd)
                    continue
                self.log.info('Balancing root %s (pools %s) by %s' %
                              (root, pools, key))
                target = best_pe.target_by_root[root]
                actual = best_pe.actual_by_root[root][key]
                queue = sorted(actual.keys(),
                               key=lambda osd: -abs(target[osd] - actual[osd]))
                for osd in queue:
                    if orig_osd_weight[osd] == 0:
                        self.log.debug('skipping out osd.%d', osd)
                    else:
                        deviation = target[osd] - actual[osd]
                        if deviation == 0:
                            break
                        self.log.debug('osd.%d deviation %f', osd, deviation)
                        weight = best_ws[osd]
                        ow = orig_osd_weight[osd]
                        if actual[osd] > 0:
                            calc_weight = target[osd] / actual[osd] * weight * ow
                        else:
                            # not enough to go on here... keep orig weight
                            calc_weight = weight / orig_osd_weight[osd]
                        new_weight = weight * (1.0 - step) + calc_weight * step
                        self.log.debug('Reweight osd.%d %f -> %f', osd, weight,
                                       new_weight)
                        next_ws[osd] = new_weight
                        if ow < 1.0:
                            new_ow = min(1.0, max(step + (1.0 - step) * ow,
                                                  ow + .005))
                            self.log.debug('Reweight osd.%d reweight %f -> %f',
                                           osd, ow, new_ow)
                            next_ow[osd] = new_ow

                # normalize weights under this root
                root_weight = crush.get_item_weight(pe.root_ids[root])
                root_sum = sum(b for a,b in next_ws.iteritems()
                               if a in target.keys())
                if root_sum > 0 and root_weight > 0:
                    factor = root_sum / root_weight
                    self.log.debug('normalizing root %s %d, weight %f, '
                                   'ws sum %f, factor %f',
                                   root, pe.root_ids[root], root_weight,
                                   root_sum, factor)
                    for osd in actual.keys():
                        next_ws[osd] = next_ws[osd] / factor

            # recalc
            plan.compat_ws = copy.deepcopy(next_ws)
            next_ms = plan.final_state()
            next_pe = self.calc_eval(next_ms)
            next_misplaced = next_ms.calc_misplaced_from(ms)
            self.log.debug('Step result score %f -> %f, misplacing %f',
                           best_pe.score, next_pe.score, next_misplaced)

            if next_misplaced > max_misplaced:
                if best_pe.score < pe.score:
                    self.log.debug('Step misplaced %f > max %f, stopping',
                                   next_misplaced, max_misplaced)
                    break
                step /= 2.0
                next_ws = copy.deepcopy(best_ws)
                next_ow = copy.deepcopy(best_ow)
                self.log.debug('Step misplaced %f > max %f, reducing step to %f',
                               next_misplaced, max_misplaced, step)
            else:
                if next_pe.score > best_pe.score * 1.0001:
                    if bad_steps < 5 and random.randint(0, 100) < 70:
                        self.log.debug('Score got worse, taking another step')
                    else:
                        step /= 2.0
                        next_ws = copy.deepcopy(best_ws)
                        next_ow = copy.deepcopy(best_ow)
                        self.log.debug('Score got worse, trying smaller step %f',
                                       step)
                else:
                    bad_steps = 0
                    best_pe = next_pe
                    best_ws = next_ws
                    best_ow = next_ow
                    if best_pe.score == 0:
                        break
            left -= 1

        # allow a small regression if we are phasing out osd weights
        fudge = 0
        if next_ow != orig_osd_weight:
            fudge = .001

        if best_pe.score < pe.score + fudge:
            self.log.info('Success, score %f -> %f', pe.score, best_pe.score)
            plan.compat_ws = best_ws
            for osd, w in best_ow.iteritems():
                if w != orig_osd_weight[osd]:
                    self.log.debug('osd.%d reweight %f', osd, w)
                    plan.osd_weights[osd] = w
            return True
        else:
            self.log.info('Failed to find further optimization, score %f',
                          pe.score)
            return False


    def calculate_optimal_weight(self, osds, device_class, stats):
        self.log.error(device_class + ': calculate optimal weight %d' % len(osds))

        norm_mode = self.cfg.get(device_class, 'reweight_normalization_mode')
        norm_avg_base = self.cfg.get(device_class, 'reweight_normalization_avg_base')

        # calculate usage, average usage, min and max usage
        total_used = 0
        total_size = 0
        total_avail = 0
        usages = []
        for _,osd in osds.iteritems():
            used = 1.0 * osd['used']
            size = 1.0 * osd['size']
            avail = 1.0 * osd['avail']
            total_used += used
            total_size += size
            total_avail += avail
            usage = used/size
            osd['usage'] = usage
            usages.append(usage)

        if total_size == 0:
            self.log.error('Total size is zero')
            return False

        avg_usage = total_used/total_size
        stats['avg_usage'] = avg_usage
        stats['mean'] = np.mean(usages)
        stats['stddev'] = np.std(usages)
        stats['cv'] = stats['stddev'] / stats['mean']

        self.log.error(device_class + ': total_size=%d, total_used=%d, total_avail=%d' % (total_size, total_used, total_avail))
        self.log.error(device_class + ': avg_usage=%f, mean=%f, std=%f, cv=%f' % (avg_usage, stats['mean'], stats['stddev'], stats['cv']))

        # calculate usage difference
        for _,osd in osds.iteritems():
            osd['usage_diff'] = osd['usage'] - avg_usage
            osd['usage_mult'] = osd['usage'] / avg_usage

        # calculate some kind of optimal weight
        max_optimal_weight = 0.0
        avg_optimal_weight = 0.0
        for _,osd in osds.iteritems():
            optimal_weight = osd['current_weight'] * avg_usage / osd['usage']
            osd['optimal_weight'] = optimal_weight
            avg_optimal_weight += optimal_weight
            if max_optimal_weight < optimal_weight:
                max_optimal_weight = optimal_weight
        avg_optimal_weight /= len(osds)

        self.log.error(device_class + ': max_optimal_weight=%f' % max_optimal_weight)

        if norm_mode == 'max':
            # normalize optimal_weight for max=1.0 with saved relative ratio
            for _,osd in osds.iteritems():
                osd['optimal_weight'] = osd['optimal_weight'] / max_optimal_weight
        elif norm_mode == 'avg':
            # normalize optimal_weight for avg=norm_avg_base with saved relative ratio
            for _,osd in osds.iteritems():
                osd['optimal_weight'] = osd['optimal_weight'] / avg_optimal_weight * norm_avg_base
        elif norm_mode == 'none':
            pass
        else:
            self.log.error('Unknown normalization mode "%s", skip step' % norm_mode)
            return False

        for _,osd in osds.iteritems():
            osd['reweight_step'] = osd['optimal_weight'] - osd['current_weight']

        return True


    def calculate_reweights(self, osds, device_class):
        self.log.error(device_class + ': calculate reweights %d' % len(osds))
        stats = {}
        if not self.calculate_optimal_weight(osds, device_class, stats):
            return {}

        max_usage_difference = self.cfg.get(device_class, 'max_usage_difference')
        abs_max_difference = self.cfg.get(device_class, 'abs_max_difference')
        max_reweight_step_inc = self.cfg.get(device_class, 'max_reweight_step_inc')
        max_reweight_step_dec = self.cfg.get(device_class, 'max_reweight_step_dec')
        cv_max = self.cfg.get(device_class, 'cv_max')
        min_avg_usage = self.cfg.get(device_class, 'min_avg_usage')
        min_osd_usage_diff = self.cfg.get(device_class, 'min_osd_usage_diff')
        min_osd_usage_mult_diff = self.cfg.get(device_class, 'min_osd_usage_mult_diff')

        do_reweight = False

        # check if exists osds with usage over max_usage_difference
        if not do_reweight:
            for osd_id,osd in osds.iteritems():
                if abs_max_difference:
                    if abs(osd['usage_diff']) > max_usage_difference:
                        do_reweight = True
                else:
                    if osd['usage_diff'] > max_usage_difference:
                        do_reweight = True

        # check if cv is exceeded
        if not do_reweight and stats['avg_usage'] > min_avg_usage:
            if stats['cv'] > cv_max:
                do_reweight = True

        if not do_reweight:
            self.log.error(device_class + ': all osd usage difference under %f' % max_usage_difference)
            return {}

        # normalize reweight_step
        max_abs_reweight_step = max([abs(el[1]['reweight_step']) for el in osds.iteritems()])
        step_scale_inc = max_reweight_step_inc / max_abs_reweight_step
        step_scale_dec = max_reweight_step_dec / max_abs_reweight_step
        self.log.error(device_class + ': max_abs_reweight_step=%f, step_scale_inc=%f, step_scale_dec=%f' % (max_abs_reweight_step, step_scale_inc, step_scale_dec))

        new_weights = {}
        # apply reweight step to current weights
        for osd_id,osd in osds.iteritems():
            if abs(osd['usage_diff']) > min_osd_usage_diff or abs(osd['usage_mult'] - 1.0) > min_osd_usage_mult_diff:
                reweight_step = osd['reweight_step']
                if reweight_step >= 0:
                    if step_scale_inc < 1.0:
                        reweight_step *= step_scale_inc
                else:
                    if step_scale_dec < 1.0:
                        reweight_step *= step_scale_dec
                target_weight = osd['current_weight'] + reweight_step
                target_weight = min(target_weight, 1.0)
                if target_weight != osd['current_weight']:
                    new_weights[osd_id] = target_weight
                    self.log.error(device_class + ': osd.%d (%f, %f, %f, %f) %f -> %f' %
                                (osd_id,
                                    osd['usage'], osd['usage_diff'], osd['optimal_weight'], osd['reweight_step'],
                                    osd['current_weight'], target_weight))
            else:
                self.log.error(device_class + ': osd.%d (%f, %f, %f, %f) skipped for small usage diff' %
                            (osd_id,
                                osd['usage'], osd['usage_diff'], osd['optimal_weight'], osd['reweight_step']))
        return new_weights


    def collect_osd_by_class(self, plan):
        ms = plan.initial

        # convert list to dict by osd_id
        osd_stats = ms.pg_dump.get('osd_stats')
        if not osd_stats:
            self.log.error('No osd stats')
            return False
        if len(osd_stats) == 0:
            self.log.error('Empty osd stats')
            return False
        stats_by_osd = {}
        for stat in osd_stats:
            stats_by_osd[stat['osd']] = stat

        stats_sanity_kb = self.cfg.get('stats_sanity_kb')

        # group osds by device_class and filter up and in osds
        insane_stats = False
        osds = {osd['osd']:osd for osd in ms.osdmap_dump.get('osds',[])}
        devices = ms.crush_dump['devices']
        self.log.error('collected %d devices, %d stats' % (len(devices), len(stats_by_osd)))
        for device in devices:
            osd_id = device['id']
            if osd_id not in osds:
                self.log.error('No osd: ' + json.dumps(device))
                continue
            osd = osds[osd_id]
            if osd['in'] != 1 or osd['up'] != 1:
                self.log.error('skip osd.%d in=%d up=%d' % (osd_id, osd['in'], osd['up']))
                continue
            if 'class' not in device:
                self.log.error('No osd class: ' + json.dumps(device))
                continue
            if device['class'] not in plan.osd_by_device_class:
                plan.osd_by_device_class[device['class']] = {}
            stat = stats_by_osd.get(osd_id)
            if stat:
                plan.osd_by_device_class[device['class']][osd_id] = {
                    'used'  : stat['kb_used'],
                    'avail' : stat['kb_avail'],
                    'size'  : stat['kb'],
                    'current_weight' : osd['weight'],
                }
                if abs(stat['kb'] - (stat['kb_used'] + stat['kb_avail'])) > stats_sanity_kb:
                    self.log.error('skip plan, insane stats: osd.%d : size=%d, avail=%d, used=%d'
                                  % (osd_id, stat['kb'], stat['kb_avail'], stat['kb_used']))
                    insane_stats = True
            else:
                self.log.error('no stats osd.%d' % osd_id)

        return not insane_stats


    def optimize_reweight(self, plan):
        # collect all required data, filter and prerocess
        if not self.collect_osd_by_class(plan):
            return False

        self.log.error('device classes: ' + ', '.join(['%s: %d' % (key, len(val)) for key,val in plan.osd_by_device_class.iteritems()]))

        # filter reweights while we must limit number of reweighted osds
        for device_class,osds in plan.osd_by_device_class.iteritems():
            if self.cfg.get(device_class, "active") and len(osds) > 0:
                reweights = self.calculate_reweights(osds, device_class)
                self.log.error(device_class + ': calculated new reweights: %d' % len(reweights))
                for osd_id,w in reweights.iteritems():
                    plan.osd_weights[osd_id] = w

        x = len(plan.osd_weights)
        if x > 0:
            self.log.error('plan reweight %d osds' % x)
        else:
            self.log.error('no osd to reweight')

        return x > 0


    def get_compat_weight_set_weights(self):
        # enable compat weight-set
        self.log.debug('ceph osd crush weight-set create-compat')
        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd crush weight-set create-compat',
            'format': 'json',
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.error('Error creating compat weight-set')
            return

        result = CommandResult('')
        self.send_command(result, 'mon', '', json.dumps({
            'prefix': 'osd crush dump',
            'format': 'json',
        }), '')
        r, outb, outs = result.wait()
        if r != 0:
            self.log.error('Error dumping crush map')
            return
        try:
            crushmap = json.loads(outb)
        except:
            raise RuntimeError('unable to parse crush map')

        raw = crushmap.get('choose_args',{}).get('-1', [])
        weight_set = {}
        for b in raw:
            bucket = None
            for t in crushmap['buckets']:
                if t['id'] == b['bucket_id']:
                    bucket = t
                    break
            if not bucket:
                raise RuntimeError('could not find bucket %s' % b['bucket_id'])
            self.log.debug('bucket items %s' % bucket['items'])
            self.log.debug('weight set %s' % b['weight_set'][0])
            if len(bucket['items']) != len(b['weight_set'][0]):
                raise RuntimeError('weight-set size does not match bucket items')
            for pos in range(len(bucket['items'])):
                weight_set[bucket['items'][pos]['id']] = b['weight_set'][0][pos]

        self.log.debug('weight_set weights %s' % weight_set)
        return weight_set

    def do_crush(self):
        self.log.info('do_crush (not yet implemented)')

    def do_osd_weight(self):
        self.log.info('do_osd_weight (not yet implemented)')

    def execute(self, plan):
        self.log.error('Executing plan %s' % plan.name)

        commands = []

        # compat weight-set
        if len(plan.compat_ws) and \
           '-1' not in plan.initial.crush_dump.get('choose_args', {}):
            self.log.debug('ceph osd crush weight-set create-compat')
            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd crush weight-set create-compat',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error creating compat weight-set')
                return

        for osd, weight in plan.compat_ws.iteritems():
            self.log.info('ceph osd crush weight-set reweight-compat osd.%d %f',
                          osd, weight)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd crush weight-set reweight-compat',
                'format': 'json',
                'item': 'osd.%d' % osd,
                'weight': [weight],
            }), 'foo')
            commands.append(result)

        # new_weight
        reweightn = {}
        for osd, weight in plan.osd_weights.iteritems():
            reweightn[str(osd)] = str(int(weight * float(0x10000)))
        if len(reweightn):
            self.log.error('ceph osd reweightn %s', reweightn)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd reweightn',
                'format': 'json',
                'weights': json.dumps(reweightn),
            }), 'foo')
            commands.append(result)

        # upmap
        incdump = plan.inc.dump()
        for pgid in incdump.get('old_pg_upmap_items', []):
            self.log.info('ceph osd rm-pg-upmap-items %s', pgid)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd rm-pg-upmap-items',
                'format': 'json',
                'pgid': pgid,
            }), 'foo')
            commands.append(result)

        for item in incdump.get('new_pg_upmap_items', []):
            self.log.info('ceph osd pg-upmap-items %s mappings %s', item['pgid'],
                          item['mappings'])
            osdlist = []
            for m in item['mappings']:
                osdlist += [m['from'], m['to']]
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd pg-upmap-items',
                'format': 'json',
                'pgid': item['pgid'],
                'id': osdlist,
            }), 'foo')
            commands.append(result)

        # wait for commands
        self.log.error('commands %s' % commands)
        for result in commands:
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error on command')
                return
        self.log.error('done')
