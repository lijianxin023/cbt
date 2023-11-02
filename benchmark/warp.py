import common
import settings
import monitoring
import os
import threading
import time
import logging
import pathlib
import client_endpoints_factory

from .benchmark import Benchmark

logger = logging.getLogger("cbt")

class WarpThread(threading.Thread):
    def __init__(self, nodes):
        threading.Thread.__init__(self, name='WarpThread' )
        self.nodes = nodes

    def run(self):
        common.pdsh(settings.getnodes(self.nodes), 'unset http_proxy;unset HTTP_PROXY; warp client').communicate()

    def __str__(self):
        return 'warp thrd %s' % (self.host)

    # this is intended to be called by parent thread after join()
    def postprocess(self):
        if not (self.exc is None):
            logger.error('thread %s: %s' % (self.name, str(self.exc)))
            raise Exception('OSD %s creation did not complete' % self.osdnum)
        logger.info('thread %s completed creation of OSD %d elapsed time %f' % (self.name, self.osdnum, self.response_time))

class Warp(Benchmark):

    def __init__(self, archive_dir, cluster, config):
        super(Warp, self).__init__(archive_dir, cluster, config)
        self.cmd_path = config.get('cmd_path', '/usr/local/bin/warp')
        self.tmp_conf = self.cluster.tmp_conf
        self.op = config.get('op', None)
        self.objsize = config.get('obj_size', None)
        self.duration = config.get('time', '5m')
        self.debug = config.get('debug', False)

        self.bucket = config.get('bucket', None)
        self.bucket_num = config.get('bucket_num', 1)
        self.bucket_prefix = config.get('bucket_prefix', None)
        self.max_keys = config.get('max_keys', None)
        self.objects = config.get('objects', None)
        self.object_prefix = config.get('object_prefix', None)
        self.per_client_object_prefix = config.get('per_client_object_prefix', True)
        self.region = config.get('region', None)
        self.report_intervals = config.get('report_intervals', None)
        self.concurrent = config.get('concurrent', None)
        self.benchdata = config.get('outpath', None)
        self.clean = config.get('clean', True)
        self.get_distrib = config.get('get_distrib', 80)
        self.put_distrib = config.get('put_distrib', 20)
        self.stat_distrib = config.get('stat_distrib', 0)
        self.delete_distrib = config.get('delete_distrib', 0)

        self.restart_pools = config.get('restart_pools', False)
        self.list_existing = config.get('list_existing', False)
        self.out_dir = self.archive_dir

        self.server = config.get("server", None)
        self.hosts = config.get("hosts", None)
        self.clients = config.get("clients", None)
        self.client_nodes = config.get("client_nodes", 'clients')
        self.client_endpoints = config.get("client_endpoints", None)
        self.objects = config.get("objects", None)
        self.analyze_skip = config.get("analyze_skip", None)
        self.analyze_detail = config.get("analyze_detail", False)
        
        self.access_key = self.config.get('access_key', '03VIHOWDVK3Z0VSCXBNH')
        self.secret_key = self.config.get('secret_key', 'KTTxQIIJV3uNox21vcqxWIpHMUOApWVWsJKdHwgG')
        self.prefill_flag = config.get('prefill', False)
        self.prefill_objsize = config.get('prefill_objsize', '1MiB')
        self.prefill_num = config.get('prefill_num', None)
        self.prefill_buckets = config.get('prefill_buckets', 1)
        self.bucket_prefill = config.get('bucket_prefill', False)
        self.bucket_prefill_num = config.get('bucket_prefill_num', 10000)

        if self.hosts is None:
            raise ValueError('No client_endpoints defined!')
        self.roundrobin = config.get("roundrobin", False)
        self.host_list = self.hosts.split(',')
        self.separate_flags = config.get("separate_flags", False)
        self.hosts_num = len(self.host_list)

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    # Initialize may only be called once depending on rebuild_every_test setting
    def initialize(self):
        super(Warp, self).initialize()
        client_endpoints_factory.reset()

        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def initialize_endpoints(self):
        super(Warp, self).initialize_endpoints()
        if self.client_endpoints is None:
            raise ValueError('No client_endpoints defined!')
        self.client_endpoints_object = client_endpoints_factory.get(self.cluster, self.client_endpoints)

        if not self.client_endpoints_object.get_initialized():
            self.client_endpoints_object.initialize()

        self.endpoint_type = self.client_endpoints_object.get_endpoint_type()
        self.endpoints_per_client = self.client_endpoints_object.get_endpoints_per_client()
        self.endpoints = self.client_endpoints_object.get_endpoints()

    def run_command(self, ep_num, host_num, cmd):
        outfile = '%s/outdata.%d.%d' % (self.run_dir, ep_num, host_num)
        outcome = '%s/output.%d.%d' % (self.run_dir, ep_num, host_num)

        cmd = 'sudo %s' % cmd
        if self.op:
            cmd += ' %s' %self.op
            if self.op == 'mixed':
                cmd += ' --get-distrib %d ' %self.get_distrib
                cmd += ' --put-distrib %d ' %self.put_distrib
                cmd += ' --stat-distrib %d ' %self.stat_distrib
                cmd += ' --delete-distrib %d ' %self.delete_distrib
        else:
            raise ValueError('No operation!')
        if self.duration:
            cmd += ' --duration %s' % self.duration
        if self.concurrent:
            cmd += ' --concurrent %d' % self.concurrent
        if self.objsize:
            cmd += ' --obj.size %s' % self.objsize
        if not self.clean:
            cmd += ' --noclear '
        if self.list_existing:
            if self.op == 'get':
                cmd += ' --list-existing '
        if self.objects:
            if self.op == 'get':
                cmd += ' --objects %d ' %self.objects
        if self.analyze_skip:
            cmd += ' --analyze.skip %s ' %self.analyze_skip

        if self.clients:
            cmd += ' --warp-client=%s' % self.clients
        if self.analyze_detail:
            cmd += ' --analyze.v '
        
        if self.debug:
            cmd += ' --debug '   
        
        if self.separate_flags:
            cmd += ' --host=%s' % self.host_list[host_num]
        else:
            cmd += ' --host=%s' % self.hosts

        if self.roundrobin:
            cmd += ' --host-select=roundrobin'

        cmd += ' --bucket warp-bucket-%d-%d-`hostname -f` ' % (ep_num, host_num)
        #cmd += ' --bucket warp-bucket-%d-%d ' % (ep_num, host_num)
        cmd += ' --benchdata %s' % outfile

        cmd += ' --access-key=%s' % self.access_key
        cmd += ' --secret-key=%s' % self.secret_key

        cmd += ' > %s' % outcome
        return cmd
    
    def prefill_command(self, ep_num, host_num, cmd, objsize, prefill_num, bucketname):
        prefill_outfile = '%s/prefill.%d' % (self.temp_res_dir, ep_num)
        common.pdsh(settings.getnodes('clients'), 'mkdir -p %s' % self.temp_res_dir).communicate()
        cmd = 'sudo %s' % cmd

        cmd += ' get '
        cmd += ' --obj.size %s ' % objsize
        cmd += ' --objects %d ' % prefill_num
        cmd += ' --noclear '
        cmd += ' --concurrent 64 '
        cmd += ' --duration 5s'

        cmd += ' --access-key=%s' % self.access_key
        cmd += ' --secret-key=%s' % self.secret_key

        cmd += ' --bucket warp-%s-%d-%d-`hostname -f` ' % (bucketname, ep_num, host_num)
        cmd += ' --benchdata %s' % prefill_outfile

        #if self.separate_flags:
        #   cmd += ' --host=%s' % self.host_list[host_num]
        #else:
        cmd += ' --host=%s' % self.hosts
        return cmd

    def prefill(self):
        super(Warp, self).prefill()
        if not self.prefill_flag:
            return
        if self.prefill_num is None:
            raise ValueError('No prefill num defined!')
        logger.info('Attempting to prefill warp objects...')
        ps = []
        for i in range(self.prefill_buckets):
            p_command="unset http_proxy;unset HTTP_PROXY; " + str(self.prefill_command
            (i, 0, self.cmd_path, self.prefill_objsize, self.prefill_num, 'prefill'))
            p = common.pdsh(settings.getnodes('clients'), p_command, False)
            ps.append(p)
        for p in ps:
            p.wait()

    def run(self):
        super(Warp, self).run()

        common.make_remote_dir(self.run_dir)

        # We'll always drop caches
        self.dropcaches()

        # dump the cluster config
        self.cluster.dump_config(self.run_dir)
        self.cluster.dump_df(self.run_dir)
        self.cluster.dump_pg(self.run_dir)
        self.cluster.dump_health(self.run_dir)

        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        if self.clients:
            thrd = WarpThread(self.client_nodes)
            logger.info('Warp client listening.')
            thrd.start()
            time.sleep(1)
        ps = []
        if self.bucket_prefill:
            logger.info('Running warp bucket prefill.')
            if not self.clients:
                for i in range(self.bucket_num):
                    if self.separate_flags:
                        for j in range(self.hosts_num):
                            p_command="unset http_proxy;unset HTTP_PROXY; " + str(self.prefill_command
                            (i, j, self.cmd_path, self.objsize, self.bucket_prefill_num, 'bucket'))
                            p = common.pdsh(settings.getnodes('clients'), p_command, False)
                            ps.append(p)
                    else:
                        p_command="unset http_proxy;unset HTTP_PROXY; " + str(self.prefill_command
                        (i, 0, self.cmd_path, self.objsize, self.bucket_prefill_num, 'bucket'))
                        p = common.pdsh(settings.getnodes('clients'), p_command, False)
                        ps.append(p)
                for p in ps:
                        p.wait()
        monitoring.start(self.run_dir)
        logger.info('Running warp test.')
        ps = []
        if self.clients:
            p = common.sh(self.server, self.run_command(0, 0, self.cmd_path), False)
            p.wait()
        else:
            for i in range(self.bucket_num):
                if self.separate_flags:
                    for j in range(self.hosts_num):
                        p = common.pdsh(settings.getnodes('clients'), "unset http_proxy;unset HTTP_PROXY; "+str(self.run_command(i, j, self.cmd_path)), True)
                        ps.append(p)
                else:
                    p = common.pdsh(settings.getnodes('clients'), "unset http_proxy;unset HTTP_PROXY; "+str(self.run_command(i, 0, self.cmd_path)), True)
                    ps.append(p)
            for p in ps:
                    p.wait()
        logger.info('Warp test done.')
        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        if self.restart_pools:
                self.cluster.restart_rgw_pools()

    def recovery_callback(self):
        self.cleanup()

    def cleanup(self):
        cmd_name = pathlib.PurePath(self.cmd_path).name
        common.pdsh(settings.getnodes('clients'), 'sudo killall -9 %s' % cmd_name).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Warp, self).__str__())
