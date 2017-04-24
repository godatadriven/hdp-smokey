import time

import ambari.api as api
from core.base_smoketest import BaseSmokeTest


class SmokeTest(BaseSmokeTest):
    """
    SmokeTest is the default implementation of the tier8 (HDP components) smoketest functionality

    The default implementation is based on the following steps:
    - step 1: Check if all component's services are started
    - step 2: Verify the functioning of the component with it's Verifiers
    - step 3: Request from AMBARI which hosts the component is running on and choose a random one
    - step 4: Stop the component
    - step 5: Execute the verification multiple times
    - step 6: Start the stopped component again
    - step 7: Verify the functioning of the component with it's Verifiers
    """

    def __init__(self, service, component, logname=None, stop_type='AMBARI', process_user=None,
                 process_indicator=None, stop_realization_timeout=5, verify_loop_sleep_time=2, verification_count=10,
                 kill_realization_timeout=30, filter_component_state=None):
        """
        Instantiate the smoketest class

        :param service: The HDP component name ('KNOX' for example)
        :param component: The HDP component name ('KNOX_GATEWAY' for example)
        :param logname: Log file name
        :param stop_type: Stop type. One of ['AMBARI', 'KILL']. Other values are ignored (process isn't stopped)
        :param process_user: The user that runs the component's component. required if type is 'KILL'
        :param process_indicator: The unique identifier of the process in the 'ps -u <user> -f | grep <indicator>'
        command
        :param stop_realization_timeout: Sleep a while to let the stopping of the component sink in
        :param verify_loop_sleep_time: Sleep time between verifications
        :param verification_count: Number of times the verification needs to take place
        :param kill_realization_timeout: Let ambari realize the process has been killed to prevent the start command
        :param filter_component_state: Filter out component by state (e.g. "ACTIVE"/"STANDBY") when picking random host to shutdown
        being ignored
        """

        super().__init__(component, logname=logname, stop_type=stop_type, process_user=process_user, process_indicator=process_indicator, stop_realization_timeout=stop_realization_timeout, verify_loop_sleep_time=verify_loop_sleep_time, verification_count=verification_count)
        self.service = service
        self.kill_realization_timeout = kill_realization_timeout
        self.filter_component_state = filter_component_state
        self.ambari = api.Api(logger=self.logger)

    def _all_started(self):
        """
        Check in AMBARI if all components have the STARTED state

        :return: True if all components are started
        """
        return self.ambari.check_if_all_components_started(self.service, self.component)

    def _get_component_info(self):
        """
        Ask AMBARI where the components are running

        :return: List of locations
        """
        return self.ambari.get_component_info(self.service, self.component)

    def _get_random_component(self, components):
        """
        Get a random path for the component from AMBARI

        :return: component_path
        """
        if self.filter_component_state is not None:
            info = {}
            host_hastate = self.ambari.get_component_host_hastate(self.service, self.component)
            filter_hosts = [host for host, hastate in host_hastate.items() if hastate == self.filter_component_state]
            info['host_components'] = [host for host in components['host_components'] if host['HostRoles']['host_name'] not in filter_hosts]
        else:
            info = components

        return self.ambari.get_random_host_and_component_path(info)

    def stop_component(self, host, location):
        """
        Stop the component

        Use the AMBARI Api to stop the component

        :param location: The full AMBARI API path for the component
        """
        self.ambari.change_host_component_state_and_wait(location, state='INSTALLED')

    def start_component(self, host, location):

        self.logger.info("Starting the previously stopped {0}".format(self.component))
        if self.stop_type == 'KILL':
            self.logger.info(
                            "Wait a while for AMBARI to realize that the component is down " +
                            "otherwise it will just ignore the start request")
            time.sleep(self.kill_realization_timeout)
        self.ambari.change_host_component_state_and_wait(location, state='STARTED')
        self.logger.info("Started the {0}".format(self.component))
