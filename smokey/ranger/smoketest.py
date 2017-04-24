import sys
import logging

from ranger import Ranger

import ambari.api as api
import hdfs.hdfs as hdfs


def initialize_logging():
    global logger
    logging.basicConfig(level=logging.DEBUG,
                        format='{asctime} ({levelname}) {funcName}(): {message}',
                        style="{",
                        filename='smoketest-ranger.log')
    logger = logging.getLogger(__name__)
    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setLevel(logging.INFO)
    errHandler = logging.StreamHandler()
    errHandler.setLevel(logging.WARNING)
    logger.addHandler(consoleHandler)
    logger.addHandler(errHandler)
    initial_log_message = """Starting Ranger smoketest. This smoketest will:
1. Turn off Ranger admin
2. Turn off HDFS namenode
3. Turn on HDFS namenode
4. Turn on Ranger

The test checks if the namenode can stop/start without dependency on Ranger
"""
    logger.info(initial_log_message)


if __name__ == "__main__":
    initialize_logging()
    ambari = api.Api(logger=logger)
    hdfs = hdfs.Hdfs()
    clustername = 'Sandbox'

    try:
        ranger_admin_ambari_info = ambari.get_component_info('RANGER', 'RANGER_ADMIN')
        hdfs_namenode_ambari_info = ambari.get_component_info('HDFS', 'NAMENODE')

        if ambari.check_if_all_components_started('RANGER', 'RANGER_ADMIN') and \
                ambari.check_if_all_components_started('HDFS', 'NAMENODE'):
            logger.info("Both HDFS namenodes and Ranger admin are in normal state. Starting smoke test.")
            rnd_ranger_host, rnd_ranger_component = ambari.get_random_host_and_component_path(ranger_admin_ambari_info)
            logger.info("Selected random Ranger admin host & components for this test: {0}, {1}"
                        .format(rnd_ranger_host,
                                rnd_ranger_component))

            logger.info("Stopping Ranger admin")
            ambari.change_host_component_state_and_wait(rnd_ranger_component, state='INSTALLED')
            logger.info("Stopped the Ranger admin")

            logger.info("Stopping standby namenode")
            standby_namenode_host = hdfs.get_standby_namenode()
            standby_namenode_path = "{0}/hosts/{1}/host_components/NAMENODE".format(clustername, standby_namenode_host)
            ambari.change_host_component_state_and_wait(standby_namenode_path, state='INSTALLED')
            logger.info("Stopped the standby namenode")

            namenode_start_timeout = 1800
            logger.debug(
                "Starting the previously stopped namenode. In HDP 2.5 this takes +-12 minutes" +
                " since Ranger is first polled 75 times. Set max timeout to {0} minutes.".format(
                    namenode_start_timeout / 60))
            ambari.change_host_component_state_and_wait(standby_namenode_path, state='STARTED',
                                                        polling_timeout=namenode_start_timeout)
            logger.debug("Started the namenode")

            logger.debug("Starting the previously stopped Ranger admin")
            ambari.change_host_component_state_and_wait(rnd_ranger_component, state='STARTED')
            logger.debug("Started the Ranger admin")

            hdfs_namenode_all_started = ambari.check_if_all_components_started('HDFS', 'NAMENODE')
            logger.debug("All namenodes in normal state? {0}".format(hdfs_namenode_all_started))

            ranger_admin_all_started = ambari.check_if_all_components_started('RANGER', 'RANGER_ADMIN')
            logger.debug("All Ranger admins in normal state? {0}".format(ranger_admin_all_started))

            logger.info("Smoke test finished {0}successfully".format("" if ranger_admin_all_started else "un"))
            sys.exit(0)
        else:
            logger.error("Not all Ranger admins in normal state. ABORT smoke test!")
            sys.exit(1)
    except api.AmbariRequestError:
        logger.error("Error requesting action from Ambari!")
        sys.exit(1)
