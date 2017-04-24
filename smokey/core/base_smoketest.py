import logging
import sys
import time
import random

import command.command
from core.verification import Verifier, VerificationError


class KillError(Exception):
    pass


class BaseSmokeTest:
    """
    BaseSmokeTest is the generic implementation of the smoketest functionality

    This implementation is based on the following steps:
    - step 1: Check if all testable components are started
    - step 2: Verify the functioning of the component with it's Verifiers
    - step 3: Choose a random component
    - step 4: Stop the component
    - step 5: Execute the verification multiple times
    - step 6: Start the stopped component again
    - step 7: Verify the functioning of the component with it's Verifiers
    """

    def __init__(self, component, logname=None, stop_type=None, process_user=None,
                 process_indicator=None, stop_realization_timeout=5, verify_loop_sleep_time=2, verification_count=10):
        """
        Instantiate the smoketest class

        :param component: The component name ('prometheus' of 'DATANODE' for example)
        :param logname: Log file name
        :param stop_type: Stop type. How to stop the component ('KILL' for example)
        :param process_user: The user that runs the component's component. required if type is 'KILL'
        :param process_indicator: The unique identifier of the process in the 'ps -u <user> -f | grep <indicator>'
        command
        :param stop_realization_timeout: Sleep a while to let the stopping of the component sink in
        :param verify_loop_sleep_time: Sleep time between verifications
        :param verification_count: Number of times the verification needs to take place
        """
        self.component = component
        self.logger = self._initialize_logging(logname)
        self.stop_type = stop_type
        self.process_user = process_user
        self.process_indicator = process_indicator
        self.stop_realization_timeout = stop_realization_timeout
        self.verify_loop_sleep_time = verify_loop_sleep_time
        self.verification_count = verification_count
        self.verifiers = Verifier(logger=self.logger)

        if self.stop_type == 'KILL':
            if self.process_user is None or self.process_indicator is None:
                raise Exception("Smoketest of type KILL cannot exist without a process user and/or process indicator")

    @staticmethod
    def _initialize_logging(logname):
        """
        Initialize the logging

        Debug logging is sent to the file, INFO logging to stdout and WARNING/ERROR to stderr

        :param logname: Path to the log file
        :return: logger instance
        """
        if logname is None:
            return logging
        else:
            logging.basicConfig(level=logging.DEBUG,
                                format='{asctime} ({levelname}) {funcName}(): {message}',
                                style="{",
                                filename=logname)

            logger = logging.getLogger(__name__)

            # Setup logging. All go to file, INFO go to stdout, WARN and ERR go to stderr
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            error_handler = logging.StreamHandler()
            error_handler.setLevel(logging.WARNING)

            logger.addHandler(console_handler)
            logger.addHandler(error_handler)

            return logger

    def _all_started(self):
        """
        Check on all hosts if the component is running

        :return: True if all components are running
        """
        return False

    def _get_component_info(self):
        """
        Override to get locations where the components are running

        :return: List of locations
        """
        return []

    def _get_random_component(self, components):
        """
        Get a random component (a host for example where one of the components is running)

        :return: single element from the list
        """
        return random.choice(components), ""

    def _stop_component(self, host, location):
        """
        Stop the component

        :param location: The location where the component is running on
        """

        if self.stop_type == 'KILL':
            self._kill_component(host)
        elif self.stop_type == 'NOOP':
            pass
        else:
            self.stop_component(host, location)

        self.logger.info("Stopped the {0}".format(self.component))

    def _kill_component(self, host):
        """
        Stop the component component by killing it

        Sets up a ssh connection to kill the process. The smoketest user need sudo rights to execute the
        kill command as the process_user

        :param host: The host the component component is running on
        """
        try:
            cmd = command.command.Command(host, self.logger)
            pid = cmd.get_pid_for_user_with_grep_expression(self.process_user, self.process_indicator)
            cmd.kill_process_of_user_with_pid(self.process_user, pid)
        except (command.command.UnknownPidError, command.command.CommandError) as e:
            raise KillError(e)

    def stop_component(self, host, location):
        """
         Override method to implement specific stop method
        """
        pass

    def start_component(self, host, location):
        """
         Override method to implement specific stop method
        """
        pass

    def _get_verifiers(self):
        verifiers = self.get_verifiers()
        if type(verifiers) is not list:
            verifiers = [verifiers]
        return verifiers

    def get_verifiers(self):
        """
        What verifier should be used

        :return: instance of the specific Verifier class
        """
        return self.verifiers

    def run(self):
        """
        Run method to execute the smoketest steps

        :return: sys.exit(0) if all went well. Otherwise sys.exit(1)
        """
        self.logger.info("Starting smoketest {0}".format(self.component))
        try:
            if self._all_started():
                self.logger.info("All {0} services in normal state.".format(self.component))
                for verifier in self._get_verifiers():
                    verifier.verify()
                self.logger.info("Verifier OK too. Starting smoke test now!")
                rnd_host, rnd_component_location = self._get_random_component(self._get_component_info())
                self.logger.info("Stopping the {0} on {1}".format(self.component, rnd_host))
                self._stop_component(rnd_host, rnd_component_location)
                try:
                    self.do_verifications(rnd_component_location, rnd_host)

                except VerificationError as ve:
                    self.logger.error("Verifier result {0}".format(str(ve)))
                    self.logger.error("Verifier status NOT OK!!")
                    sys.exit(1)

                self.logger.info("Smoketest finished successfully")
                sys.exit(0)
            else:
                self.logger.error("Not all components in normal state. ABORTED smoke test!")
                sys.exit(1)
        except VerificationError as ve:
            self.logger.error("Verifier result {0}".format(str(ve)))
            self.logger.error("Verifier status NOT OK!! ABORTED smoke test!")
            sys.exit(1)
        except KillError as e:
            if type(e) is KillError:
                self.logger.error("Error stopping process with kill!!")
            sys.exit(1)

    def do_verifications(self, rnd_component_location, rnd_host):
        # loop
        counter = self.verification_count
        time.sleep(self.stop_realization_timeout)  # wait for other components to realize the component
        # is stopped to prevent the verifier from returning error
        while counter > 0:
            for verifier in self._get_verifiers():
                verifier.verify()
            counter -= 1
            time.sleep(self.verify_loop_sleep_time)

        # end loop
        self.logger.info("Starting the previously stopped {0}".format(self.component))
        self.start_component(rnd_host, rnd_component_location)
        self.logger.info("Started the {0}".format(self.component))
        self.logger.info(
            "All {0} components in normal state? {1}".format(self.component, self._all_started()))
        self.logger.info("Final check of {0} with the verifier".format(self.component))
        for verifier in self._get_verifiers():
            verifier.verify()
