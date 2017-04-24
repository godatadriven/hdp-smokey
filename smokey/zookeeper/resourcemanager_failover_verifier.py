from yarn.yarn_resourcemanager_smoketest import YarnResourceManagerVerifier
from yarn import yarn
from core import base
from ambari import api


class ResourceManagerFailoverVerifier(YarnResourceManagerVerifier):

    def __init__(self, logger):
        super().__init__(logger)
        self.component_state="ACTIVE"
        self.service = "YARN"
        self.component = "RESOURCEMANAGER"
        self.ambari = api.Api(logger=self.logger)

    def verify(self):
        self.logger.info("Verify YARN ResourceManager status")
        host, component_path = self._get_host_and_component_path_by_state(self.component_state)
        self.logger.info("Stopping the {0} on {1}".format(self.component, host))
        self.ambari.change_host_component_state_and_wait(component_path, state='INSTALLED')
        try:
            self.yarn.check_yarn_resourcemanager_status()
        except yarn.YarnRequestError as e:
            self.logger.error("YARN ResourceManager status NOT OK!")
            raise base.VerificationError(e)
        self.logger.info("Starting the previously stopped {0}".format(self.component))
        self.ambari.change_host_component_state_and_wait(component_path, state='STARTED')
        self.logger.info("Started the {0}".format(self.component))

    def _get_host_and_component_path_by_state(self, state):
        """
        Get a random host and path for the component from AMBARI

        :return: (hostname, component_path)
        """
        info = self.ambari.get_component_info(self.service, self.component)
        host_hastate = self.ambari.get_component_host_hastate(self.service, self.component)
        filter_hosts = [host for host, hastate in host_hastate.items() if hastate == state]
        info['host_components'] = [host for host in info['host_components'] if host['HostRoles']['host_name'] in filter_hosts]

        return self.ambari.get_random_host_and_component_path(info)


