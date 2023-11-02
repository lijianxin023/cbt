import common
import settings
import monitoring
import os
import time
import logging
import pathlib
import client_endpoints_factory

from .benchmark import Benchmark

logger = logging.getLogger("cbt")


class Vdbench(Benchmark):
    def __init__(self, archive_dir, cluster, config):
        super(Vdbench, self).__init__(archive_dir, cluster, config)

        # FIXME there are too many permutations, need to put results in SQLITE3
        self.cmd_path = config.get('cmd_path', '/root/vdbench/vdbench')
        self.direct = str(config.get('direct', 1))
        self.time = config.get('time', None)
        self.parm_file = config.get('parm_file', None)
        self.time_based = bool(config.get('time_based', False))
        self.ramp = config.get('ramp', 1)


        self.iodepth = config.get('iodepth', 16)
        self.prefill_iodepth = config.get('prefill_iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.sync = config.get('sync', None)
        self.end_fsync = config.get('end_fsync', 0)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.logging = config.get('logging', True)
        self.log_avg_msec = config.get('log_avg_msec', None)
        self.ioengine = config.get('ioengine', 'libaio')
        self.bssplit = config.get('bssplit', None)
        self.bsrange = config.get('bsrange', None)
        self.bs = config.get('bs', None)
        self.op_size = config.get('op_size', 4194304) # Deprecated, please use bs
        self.size = config.get('size', 4096)
        self.procs_per_endpoint = config.get('procs_per_endpoint', 1)
        self.random_distribution = config.get('random_distribution', None)
        self.rate_iops = config.get('rate_iops', None)
        self.out = "json,normal"

        self.prefill_flag = config.get('prefill', True)
        self.prefill_bs = config.get('prefill_bs', '1M')
        self.prefill_workload = config.get('prefill_workload', 'prefill')

        self.workload = config.get('workload', None)
        self.rdname = config.get('rdname', None)
        self.out_dir = self.archive_dir
        self.client_endpoints = config.get("client_endpoints", None)
        self.recov_test_type = config.get('recov_test_type', 'blocking')

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self):
        super(Vdbench, self).initialize()

        # Clean and Create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def initialize_endpoints(self):
        super(Vdbench, self).initialize_endpoints()

        # Get the client_endpoints and set them up
        if self.client_endpoints is None:
            raise ValueError('No client_endpoints defined!')
        self.client_endpoints_object = client_endpoints_factory.get(self.cluster, self.client_endpoints)

        # Create the recovery image based on test type requested
        if 'recovery_test' in self.cluster.config and self.recov_test_type == 'background':
            self.client_endpoints_object.create_recovery_image()
        self.create_endpoints()

    def create_endpoints(self):
        if not self.client_endpoints_object.get_initialized():
            self.client_endpoints_object.initialize()
        else:
            logger.info('already initialized, skip')

        self.endpoint_type = self.client_endpoints_object.get_endpoint_type()
        self.endpoints_per_client = self.client_endpoints_object.get_endpoints_per_client()
        self.endpoints = self.client_endpoints_object.get_endpoints()

    def prefill_command(self):
        prefill_out_dir = os.path.join(self.out_dir, 'prefill_out')
        cmd = 'sudo %s' % self.cmd_path
        cmd += ' -f %s' %self.parm_file
        cmd += ' -o %s' %prefill_out_dir
        cmd += ' workload=%s ' % self.prefill_workload
        cmd += ' time=2h '
        cmd += ' rdname=prefill_run '
        cmd += ' ramp=1 '
        return cmd

    def prefill(self):
        super(Vdbench, self).prefill()
        if not self.prefill_flag:
            return
        logger.info('Attempting to prefill fio files...')
        p = common.pdsh(settings.getnodes('head'), self.prefill_command())
        p.wait()

    def run_command(self):
        out_dir = os.path.join(self.out_dir, 'vdbench_out')

        if not self.rdname:
            self.rdname = self.workload

        # cmd_path_full includes any valgrind or other preprocessors vs cmd_path
        cmd = 'sudo %s' % self.cmd_path
        cmd += ' -f %s ' % self.parm_file
        cmd += ' -o %s ' % out_dir

        # IO options
        cmd += ' workload=%s ' % self.workload
        cmd += ' time=%d' % self.time
        cmd += ' rdname=%s ' % self.rdname
        cmd += ' ramp=%d ' % self.ramp

        return cmd

    def run(self):
        super(Vdbench, self).run()

        # We'll always drop caches for rados bench
        self.dropcaches()

        # Create the run directory
        common.make_remote_dir(self.run_dir)

        # dump the cluster config
        
        self.cluster.dump_config(self.run_dir)
        self.cluster.dump_pg(self.run_dir)
        self.cluster.dump_health(self.run_dir)
        
        time.sleep(5)

        monitoring.start_with_wait(self.run_dir)

        logger.info('Running vdbench %s test.', self.workload)
        p = common.pdsh(settings.getnodes('head'), self.run_command())
        p.wait()
        # If we were doing recovery, wait until it's done.
        if 'recovery_test' in self.cluster.config:
            self.cluster.wait_recovery_done()

        monitoring.stop(self.run_dir)

        # Finally, get the historic ops
        self.cluster.dump_historic_ops(self.run_dir)
        common.sync_files('%s/*' % self.run_dir, self.out_dir)

    def cleanup(self):
        cmd_name = pathlib.PurePath(self.cmd_path).name
        common.pdsh(settings.getnodes('clients'), 'sudo killall -2 %s' % cmd_name).communicate()

    def recovery_callback_blocking(self):
        self.cleanup()

    def recovery_callback_background(self):
        logger.info('Recovery thread completed!')

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(Vdbench, self).__str__())
