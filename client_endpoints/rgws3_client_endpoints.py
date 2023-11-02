from .ceph_client_endpoints import CephClientEndpoints


class RgwS3ClientEndpoints(CephClientEndpoints):
    def __init__(self, cluster, config):
        super(RgwS3ClientEndpoints, self).__init__(cluster, config)

    def create(self):
        self.url = self.config.get('url', '9000')
        self.access_key = self.config.get('access_key', '03VIHOWDVK3Z0VSCXBNH')
        self.secret_key = self.config.get('secret_key', 'KTTxQIIJV3uNox21vcqxWIpHMUOApWVWsJKdHwgG')
        self.user = self.config.get('user', 'cbt')
        self.cluster.add_s3_user(self.user, self.access_key, self.secret_key)

    def mount(self):
        if self.use_existing:
            return
        # Don't actually mount anything, just set the endpoints
        urls = self.config.get('urls', self.cluster.get_urls())
        self.endpoint_type = "s3"
        return self.get_endpoints()
