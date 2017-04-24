import service.command
from core.base_smoketest import BaseSmokeTest


class ServiceActionError(Exception):
    pass


class ServiceSmokeTest(BaseSmokeTest):
    """
    ServiceSmokeTest is the default implementation of the tier7 (non-HDP components) smoketest functionality

    The default implementation is based on the following steps:
    - step 1: Check if all component's services are started
    - step 2: Verify the functioning of the component with it's Verifiers
    - step 3: Choose a random host
    - step 4: Stop the component's component
    - step 5: Execute the verification multiple times
    - step 6: Start the stopped component's component again
    - step 7: Verify the functioning of the component with it's Verifiers
    """

    def __init__(self, component, logname=None, stop_type='SERVICE', hosts=[], process_user=None,
                 process_indicator=None, stop_realization_timeout=5, verify_loop_sleep_time=2, verification_count=10):
        """
        Instantiate the smoketest class

        :param service: The component name ('prometheus' for example)
        :param logname: Log file name
        :param stop_type: Stop type. One of ['SERVICE', 'KILL']. Other values are ignored (process isn't stopped)
        :param hosts: Hosts where the component is supposed to be installed
        :param process_user: The user that runs the component's component. required if type is 'KILL'
        :param process_indicator: The unique identifier of the process in the 'ps -u <user> -f | grep <indicator>'
        command
        :param stop_realization_timeout: Sleep a while to let the stopping of the component sink in
        :param verify_loop_sleep_time: Sleep time between verifications
        :param verification_count: Number of times the verification needs to take place
        """
        super().__init__(component, logname=logname, stop_type=stop_type, process_user=process_user, process_indicator=process_indicator, stop_realization_timeout=stop_realization_timeout, verify_loop_sleep_time=verify_loop_sleep_time, verification_count=verification_count)
        self.hosts = hosts

    def _all_started(self):
        """
        Check on all hosts if the component is running

        :return: True if all components are running
        """
        for host in self.hosts:
            try:
                cmd = service.command.Command(host, self.logger)
                cmd.call_service_action(self.component, 'status')
            except service.command.ServiceActionError as e:
                return False
        return True

    def _get_component_info(self):
        """
        Get locations where the components are running

        :return: List of locations
        """
        return self.hosts

    def stop_component(self, host, location):
        """
        Stop the component

        Setup a ssh connection and stop the component

        :param host: The host the component component is running on
        """
        try:
            cmd = service.command.Command(host, self.logger)
            cmd.call_service_action(self.component, 'stop')
        except service.command.ServiceActionError as e:
            raise ServiceActionError(e)

    def start_component(self, host, location):
        """
        Start the component

        Setup a ssh connection and start the component. The smoketest user need sudo rights for stopping/starting the
        component or to execute the kill command as the process_user

        :param host: The host the component component is running on
        """
        try:
            cmd = service.command.Command(host, self.logger)
            cmd.call_service_action(self.component, 'start')
        except service.command.ServiceActionError as e:
            raise ServiceActionError(e)

