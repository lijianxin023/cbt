from contextlib import contextmanager
import glob
import os.path
import common
import settings
import logging
import time

logger = logging.getLogger("cbt")

def sshtarget(user, host):
    h = host
    if user:
        h = '%s@%s' % (user, host)
    return h

class Monitoring(object):
    def __init__(self, mconfig):
        # the initializers should be the very single places interrogating
        # settings for the sake of explicitness
        nodes_list = mconfig.get('nodes', self._get_default_nodes())
        self.nodes = settings.getnodes(*nodes_list)
        self.wait_time = mconfig.get('wait_time', 0)

    @staticmethod
    def _get_all():
        for monitoring, mconfig in sorted(settings.monitoring_profiles.items()):
            yield Monitoring._get_object(monitoring, mconfig)

    @staticmethod
    def _get_object(monitoring, mconfig):
        if monitoring == 'collectl':
            return CollectlMonitoring(mconfig)
        if monitoring == 'perf':
            return PerfMonitoring(mconfig)
        if monitoring == 'blktrace':
            return BlktraceMonitoring(mconfig)
        if monitoring == 'iostat':
            return IostatMonitoring(mconfig)
        if monitoring == 'sar':
            return SarMonitoring(mconfig)
        if monitoring == 'metrics':
            return MetricsMonitoring(mconfig)


class CollectlMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(CollectlMonitoring, self).__init__(mconfig)

        self.args = mconfig.get('args', '-s+mYZ -i 1:10 -F0 -f {collectl_dir} '
                                        r'--rawdskfilt \"+cciss/c\d+d\d+ |hd[ab] | sd[a-z]+ |dm-\d+ |xvd[a-z] |fio[a-z]+ | vd[a-z]+ |emcpower[a-z]+ |psv\d+ |nvme[0-9]n[0-9]+p[0-9]+ \"')

    def start(self, directory):
        collectl_dir = '%s/collectl' % directory
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % collectl_dir).communicate()
        common.pdsh(self.nodes, ['collectl', self.args.format(collectl_dir=collectl_dir)])

    def stop(self, directory):
        common.pdsh(self.nodes, 'pkill -SIGINT -f collectl').communicate()

    @staticmethod
    def _get_default_nodes():
        return ['clients', 'osds', 'mons', 'rgws']


class PerfMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(PerfMonitoring, self).__init__(mconfig)
        self.pid_dir = settings.cluster.get('pid_dir')
        self.pid_glob = mconfig.get('pid_glob', 'osd.*.pid')
        self.user = settings.cluster.get('user')
        self.args_template = mconfig.get('args')
        self.perf_runners = []
        self.perf_dir = ''  # we need the output file to extract data

    def start(self, directory):
        time.sleep(self.wait_time)
        perf_dir = '%s/perf' % directory
        self.perf_dir = perf_dir
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % perf_dir).communicate()

        perf_template = 'sudo perf {} &'.format(self.args_template)
        local_node = common.get_localnode(self.nodes)
        if local_node:
            logger.debug("PerfMonitoring: in local_node");
            logger.debug("pid_dir: %s" % self.pid_dir);
            for pid_path in glob.glob(os.path.join(self.pid_dir, self.pid_glob)):
                logger.debug("PerfMonitoring pid_path: %s" % pid_path);
                with open(pid_path) as pidfile:
                    pid = pidfile.read().strip()
                    perf_cmd = perf_template.format(perf_dir=perf_dir, pid=pid)
                    runner = common.sh(local_node, perf_cmd)
                    self.perf_runners.append(runner)
        else:
            logger.debug("PerfMonitoring: remote_node");
            # ${pid} will be handled by remote's sh
            perf_cmd = perf_template.format(perf_dir=perf_dir, pid='${pid}')
            common.pdsh(self.nodes, ['for pid in `cat %s/%s`;' % (self.pid_dir, self.pid_glob),
                                     'do', perf_cmd,
                                     'done'])

    def stop(self, directory):
        if self.perf_runners:
            for runner in self.perf_runners:
                runner.kill()
        else:
            common.pdsh(self.nodes, 'sudo pkill -SIGINT -f perf\ ').communicate()
        if directory:
            common.pdsh(self.nodes, 'sudo chown {user}.{user} {dir}/perf/perf.data'.format(
                user=self.user, dir=directory))
            common.pdsh(self.nodes, 'sudo chown {user}.{user} {dir}/perf/perf_stat.*'.format(
                user=self.user, dir=directory))

    def get_cpu_cycles(self, out_dir):
        import re
        total_cpu_cycles = 0
        perf_dir_name = str(glob.glob(out_dir + "/perf*")[0])
        perf_stat_fnames = os.listdir(perf_dir_name)
        for perf_out_fname in perf_stat_fnames:
            perf_output_file = open(perf_dir_name + "/" + perf_out_fname, "rt")
            match = re.search(r'(.*) cycles(.*?) .*', perf_output_file.read(), re.M | re.I)
            if match:
                cpu_cycles = match.group(1).strip()
            else:
                return None
            total_cpu_cycles = total_cpu_cycles + int(cpu_cycles.replace(',', ''))
        return total_cpu_cycles

    @staticmethod
    def _get_default_nodes():
        return ['osds']


class BlktraceMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(BlktraceMonitoring, self).__init__(mconfig)
        self.osds_per_node = settings.cluster.get('osds_per_node')
        self.use_existing = settings.cluster.get('use_existing', True)
        self.user = settings.cluster.get('user')

    def start(self, directory):
        blktrace_dir = '%s/blktrace' % directory
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % blktrace_dir).communicate()
        for device in range(0, self.osds_per_node):
            common.pdsh(self.nodes, 'cd %s;sudo blktrace -o device%s -d /dev/disk/by-partlabel/osd-device-%s-data'
                        % (blktrace_dir, device, device))

    def stop(self, directory):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f blktrace').communicate()
        if directory and not self.use_existing:
            self._make_movies(directory)

    def _make_movies(self, directory):
        seekwatcher = '/home/%s/bin/seekwatcher' % self.user
        blktrace_dir = '%s/blktrace' % directory

        for device in range(self.osds_per_node):
            common.pdsh(self.nodes, 'cd %s;%s -t device%s -o device%s.mpg --movie' %
                        (blktrace_dir, seekwatcher, device, device)).communicate()

    @staticmethod
    def _get_default_nodes():
        return ['osds']

class IostatMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(IostatMonitoring, self).__init__(mconfig)
        self.interval = mconfig.get('interval', None)
        self.count = mconfig.get('count', None)
        self.user = settings.cluster.get('user')
        self.args_template = mconfig.get('args')
        self.perf_runners = []
        self.iostat_dir = ''  # we need the output file to extract data

    def start(self, directory):
        time.sleep(self.wait_time)
        iostat_dir = '%s/iostat' % directory
        self.iostat_dir = iostat_dir
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % iostat_dir).communicate()

        iostat_template = 'sudo iostat {} '.format(self.args_template)
        iostat_cmd = iostat_template.format(interval=self.interval, count=self.count, iostat_dir=iostat_dir)
        logger.debug("IostatMonitoring: display %s reports at %s second intervals" % (self.count, self.interval))
        common.pdsh(self.nodes, iostat_cmd)

    def stop(self, directory):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f iostat ').communicate()

    @staticmethod
    def _get_default_nodes():
        return ['osds']

class SarMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(SarMonitoring, self).__init__(mconfig)
        self.interval = mconfig.get('interval', None)
        self.count = mconfig.get('count', None)
        self.info = mconfig.get('info', 'DEV')
        self.user = settings.cluster.get('user')
        self.args_template = mconfig.get('args')
        self.perf_runners = []
        self.sar_dir = ''  # we need the output file to extract data

    def start(self, directory):
        time.sleep(self.wait_time)
        sar_dir = '%s/sar' % directory
        self.sar_dir = sar_dir
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % sar_dir).communicate()

        sar_template = 'sudo sar {} '.format(self.args_template)
        sar_cmd = sar_template.format(info=self.info, interval=self.interval, count=self.count, sar_dir=sar_dir)
        logger.debug("SarMonitoring: display %s reports at %s second intervals" % (self.count, self.interval))
        common.pdsh(self.nodes, sar_cmd)

    def stop(self, directory):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f sar ').communicate()

    @staticmethod
    def _get_default_nodes():
        return ['clients', 'osds']

class MetricsMonitoring(Monitoring):
    def __init__(self, mconfig):
        super(MetricsMonitoring, self).__init__(mconfig)
        self.interval = mconfig.get('interval', None)
        self.user = settings.cluster.get('user')
        self.args_template = mconfig.get('args')
        self.num = mconfig.get('osd_num', 1)
        self.interval = mconfig.get('interval', 10)
        self.count = mconfig.get('count', 1)
        self.metric = mconfig.get('metric')
        self.ceph_cmd = settings.cluster.get('ceph_cmd')
        self.metrics_dir = ''  # we need the output file to extract data

    def start(self, directory):
        time.sleep(self.wait_time)
        metrics_dir = '%s/metrics' % directory
        self.metrics_dir = metrics_dir
        common.pdsh(self.nodes, 'mkdir -p -m0755 -- %s' % metrics_dir).communicate()
        logger.debug("MetricsMonitoring: display %s metric at %s second intervals" % (self.metric, self.interval))
        metrics_template = '{} {} '.format(self.ceph_cmd, self.args_template)

        osdhosts = settings.cluster.get('osds')
        osdnum = 0
        user = settings.cluster.get('user')
        for host in osdhosts:
            osd_count = settings.cluster.get('osds_per_node')
            distributed = settings.cluster.get('osds_distributed')
            osd_list = list(range(osdnum, osdnum+osd_count))
            if settings.cluster.get('osds_distributed'):
                if host in settings.cluster.get('osds_distributed'):
                        osd_list = distributed[host]
                        osd_count = len(osd_list)
            pdshhost = sshtarget(user, host)
            osd_end = osdnum + osd_count

            metrics_cmd = metrics_template.format(id='${id}', metric=self.metric, metrics_dir=self.metrics_dir)
            loop_cmd = [ 'for((i=0;i<%s;++i))'%self.count,
            'do for((id=%s;id<%s;++id)) ' %(osdnum, osd_end),
            'do sudo', metrics_cmd,
            ';done;',
            'sleep %s;' % self.interval,
            'done']
            common.pdsh(pdshhost, loop_cmd)
            osdnum = osd_end

    def stop(self, directory):
        common.pdsh(self.nodes, 'sudo pkill -SIGINT -f dump_metrics').communicate()

    @staticmethod
    def _get_default_nodes():
        return ['osds']

def start(directory):
    for m in Monitoring._get_all():
        m.start(directory)


def stop(directory=None):
    for m in Monitoring._get_all():
        m.stop(directory)


@contextmanager
def monitor(directory):
    monitors = []
    for m in Monitoring._get_all():
        m.start(directory)
        monitors.append(m)
    yield
    for m in monitors:
        m.stop(directory)


def get_cpu_cycles(out_dir):
    # check if perf stat is configured
    for monitoring_profile in Monitoring._get_all():
        if(isinstance(monitoring_profile, PerfMonitoring)):
            return monitoring_profile.get_cpu_cycles(out_dir)  # if it is, then return the number of cycle
    return None
