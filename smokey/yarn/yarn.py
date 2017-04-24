import logging
import requests

import ambari.api as ambari_api


class YarnRequestError(Exception):
    pass


class Yarn:
    def __init__(self, request_timeout=10, logger=logging):
        self.timeout = request_timeout
        self.logger = logger

        self.ambari = ambari_api.Api(logger=self.logger)

        yarn_resourcemanager_ambari_info = self.ambari.get_component_info('YARN', 'RESOURCEMANAGER')
        yarn_resourcemanager_hosts = [host['HostRoles']['host_name'] for host in
                                      yarn_resourcemanager_ambari_info['host_components']]
        self.yarn_resourcemanager_urls = ["http://{0}:8088".format(host) for host in yarn_resourcemanager_hosts]

    def check_yarn_resourcemanager_status(self):
        """
        Check YARN resourcemanager's state
        If one of the resourcemanager UIs returns HTTP status 200, it's a success, else an error is thrown
        :return:
        """
        self.logger.debug(
            "Checking YARN ResourceManager status. {0} known addresses.".format(len(self.yarn_resourcemanager_urls)))

        status_200_received = False
        for i, resourcemanager_url in enumerate(self.yarn_resourcemanager_urls):
            self.logger.debug("Checking ResourceManager #{0}: {1}".format(i + 1, resourcemanager_url))
            try:
                response = requests.get(resourcemanager_url, timeout=self.timeout)
                if response.status_code == 200:
                    status_200_received = True
            except Exception as e:
                self.logger.debug("Connection to ResourceManager {0} failed.".format(resourcemanager_url))

        if not status_200_received:
            self.logger.error("Both YARN ResourceManagers did not return HTTP status code 200!")
            raise YarnRequestError("YARN ResourceManagers did not return HTTP status code 200.")
