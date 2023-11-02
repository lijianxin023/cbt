import common
import logging
import settings

from .ceph_client_endpoints import CephClientEndpoints

logger = logging.getLogger("cbt")

class KRbdClientEndpoints(CephClientEndpoints):
    def __init__(self, cluster,config):
        super(KRbdClientEndpoints, self).__init__(cluster, config)
        self.ms_mode = config.get('ms_mode', None)

    def create(self):
        self.create_rbd()

    def mount(self):
        common.pdsh(settings.getnodes('clients'), 'sudo rm -rf %s' % self.mnt_dir, continue_if_error=False).communicate()
        for node in common.get_fqdn_list('clients'):
            info = settings.host_info(node)
            node_str = '%s@%s' %(info['user'], info['host'])
            common.pdsh(node_str, 'sudo mkdir -p -m0755 -- %s' % self.mnt_dir, continue_if_error=False).communicate() 
            for ep_num in range(0, self.endpoints_per_client):
                rbd_name = self.get_rbd_name(node, ep_num)

                rbd_device = self.map_rbd(node_str, rbd_name)
                logger.info(rbd_device)

        self.endpoint_type = "krbd"
        return self.get_endpoints()

    def map_rbd(self, node, rbd_name):
        cmd = 'sudo %s map %s/%s --id admin --options noshare' % (self.rbd_cmd, self.pool, rbd_name)
        if self.ms_mode:
            cmd += ' --options ms_mode=%s' % self.ms_mode
        stdout, stderr = common.pdsh(node, cmd, continue_if_error=False).communicate()
        return stdout.rstrip().rpartition(": ")[2]

    def create_recovery_image(self):
        self.create_rbd_recovery()
