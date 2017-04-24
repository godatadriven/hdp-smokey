import os
import logging
import requests

import ambari.api as api

from utils.utils import logmethodcall


class RangerRequestError(Exception):
    pass


class Ranger:
    def __init__(self, request_timeout=10):
        self.timeout = request_timeout
        self.ranger_schema = os.environ.get('RANGER_SCHEMA', 'http')
        self.ranger_host = os.environ.get('RANGER_HOST', 'sandbox.hortonworks.com')
        self.ranger_port = os.environ.get('RANGER_PORT', 6080)

        logging.basicConfig(level=logging.DEBUG,
                            format='{asctime} ({levelname}) {funcName}(): {message}',
                            style="{",
                            filename='ranger.log')

        self.logger = logging.getLogger(__name__)

    @logmethodcall
    def get_ranger_url(self):
        return '{0}://{1}:{2}/'.format(self.ranger_schema, self.ranger_host, self.ranger_port)

    @logmethodcall
    def is_ranger_online(self):
        try:
            requests.get(self.get_ranger_url(), timeout=self.timeout)
            return True
        except:
            return False

    @logmethodcall
    def stop_ranger_admin(self):
        ambari = api.Api(logger=self.logger)
        ranger_admin_ambari_info = ambari.get_component_info('RANGER', 'RANGER_ADMIN')
        rnd_ranger_host, rnd_ranger_component = ambari.get_random_host_and_component_path(ranger_admin_ambari_info)
        self.logger.info("Selected random Ranger admin host for stopping: {0}, {1}"
                         .format(rnd_ranger_host, rnd_ranger_component))
        ambari.change_host_component_state_and_wait(rnd_ranger_component, state='INSTALLED')

    @logmethodcall
    def check_ranger_status(self):
        ranger_url = '{0}://{1}:{2}/'.format(self.ranger_schema, self.ranger_host, self.ranger_port)
        self.logger.debug(ranger_url)
        response = requests.get(ranger_url, timeout=self.timeout)
        self.verify_ranger_response(response)

    @logmethodcall
    def verify_ranger_response(self, response):
        if response.status_code != 200:
            self.logger.error(
                "RangerResponse returned with error status [{0}], response was: {1}".format(response.status_code,
                                                                                            response.text))
            raise RangerRequestError("RangerResponse returned with error status [{0}]".format(response.status_code))
