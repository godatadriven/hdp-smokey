import json
import logging
import os
import random
import requests
import signal
import time
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class AmbariWaitForCompletionTimeoutError(Exception):
    pass


class AmbariRequestError(Exception):
    pass


class Api:
    def __init__(self, request_timeout=10, polling_timeout=60, logger=logging):
        self.timeout = request_timeout
        self.polling_timeout = polling_timeout
        self.ambari_schema = os.environ.get('AMBARI_SCHEMA', 'http')
        self.ambari_host = os.environ.get('AMBARI_HOST', 'sandbox')
        self.ambari_port = os.environ.get('AMBARI_PORT', 8080)
        self.ambari_user = os.environ.get('AMBARI_USER', 'raj_ops')
        self.ambari_pwd = os.environ.get('AMBARI_PWD', 'raj_ops')
        self.ambari_base = 'api/v1/clusters'
        self.clustername = 'Sandbox'
        self.ambari_session = None
        self.default_ambari_headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "smoketest"
        }

        self.logger = logger

    @staticmethod
    def _encode_escaped_json_string_for_ambari(data):
        return json.dumps(json.dumps(data))

    @staticmethod
    def _sig_alarm(signum, frame):
        raise AmbariWaitForCompletionTimeoutError("Timeout occurred!")

    def _request_ambari(self, path, method='GET', headers=None, **kwargs):
        if headers is None:
            headers = dict()

        if path.startswith('http'):
            ambari_url = path
        else:
            ambari_url = '{0}://{1}:{2}/{3}/{4}'.format(self.ambari_schema, self.ambari_host, self.ambari_port,
                                                        self.ambari_base, path)
        self.logger.debug(ambari_url)
        ambari_headers = {**self.default_ambari_headers, **headers}
        self.logger.debug(ambari_headers)
        r = requests.request(method, ambari_url, headers=ambari_headers, timeout=self.timeout, verify=False, **kwargs)
        return self._check_response_status(r)

    def _init_session(self):
        r = self._request_ambari('', auth=(self.ambari_user, self.ambari_pwd))
        self.logger.debug(r.cookies['AMBARISESSIONID'])
        self.ambari_session = r.cookies['AMBARISESSIONID']
        self.default_ambari_headers = {**self.default_ambari_headers,
                                       "Cookie": "AMBARISESSIONID=" + self.ambari_session}

    def _check_response_status(self, response):
        self.logger.debug(response.text)
        if response.status_code >= 400:
            self.logger.error(
                "AmbariResponse returned with error status [{0}], response was: {1}".format(response.status_code,
                                                                                            response.json()))
            raise AmbariRequestError("AmbariResponse returned with error status [{0}]".format(response.status_code))
        return response

    def _wait_for_request_completion(self, path, polling_timeout=None):
        polling_timeout = self.polling_timeout if polling_timeout is None else polling_timeout
        signal.signal(signal.SIGALRM, self._sig_alarm)
        waiting_for_completion = True
        try:
            signal.alarm(polling_timeout)
            while waiting_for_completion:
                completion_result = self.request_ambari(path).json()
                if completion_result['Requests']['request_status'] != 'COMPLETED':
                    self.logger.warning("Waiting for completion. Progress = {progress}, status = {status}".format(
                        progress=completion_result['Requests']['progress_percent'],
                        status=completion_result['Requests']['request_status']))
                    time.sleep(2)
                else:
                    self.logger.info("Finished waiting for completion.")
                    waiting_for_completion = False
                    signal.alarm(0)
        except AmbariWaitForCompletionTimeoutError:
            self.logger.error("Timeout occurred during waiting for request completion of {path}".format(path=path))
            raise AmbariWaitForCompletionTimeoutError

    def _change_host_component_state(self, path, state='STARTED'):
        self.logger.debug(path)
        self.logger.debug(state)
        ambari_data = {"RequestInfo": {"context": "Change state of " + path + " via ambari_api.py (REST)"},
                       "Body": {"HostRoles": {"state": state}}}

        self.logger.debug(ambari_data)
        return self.request_ambari(path, method='PUT', data=ambari_data)

    def _check_if_all_components_started(self, state_info):
        total_count = state_info['ServiceComponentInfo']['total_count']
        started_count = state_info['ServiceComponentInfo']['started_count']
        state = state_info['ServiceComponentInfo']['state']
        self.logger.debug("state={0}, started_count={1} and total_count={2}".format(state, started_count, total_count))
        return state == "STARTED" and total_count == started_count

    def request_ambari(self, path, method='GET', headers=None, **kwargs):
        self.logger.info("Calling Ambari API ({0})".format(path))
        if self.ambari_session is None:
            self._init_session()
        if 'data' in kwargs:
            if isinstance(kwargs['data'], dict):
                kwargs['data'] = self._encode_escaped_json_string_for_ambari(kwargs['data'])
        return self._request_ambari(path, method, headers, **kwargs)

    def get_service_components(self, service):
        self.logger.info("Getting component components from Ambari API {0}".format(service))
        r = self.request_ambari('{0}/services/{1}/components'.format(self.clustername, service))
        return r.json()

    def get_component_info(self, service, component_name):
        self.logger.info("Getting component info from Ambari API {0}/{1}".format(service, component_name))
        url = '{0}/services/{1}/components/{2}?fields=host_components'.format(self.clustername, service, component_name)
        response = self.request_ambari(url).json()
        self.logger.debug(response)
        return response

    def get_component_processes_info(self, path):
        self.logger.info("Getting component process info from Ambari API {0}".format(path))
        response = self.request_ambari("{0}/processes".format(path)).json()
        self.logger.debug(response)
        return response

    def get_component_host_hastate(self, service, component_name):
        # TODO: Works for YARN resourcemanager, but the hastate is not a generic API.
        # TODO: For Namenode we implemented get_specific_nn_host method.
        # TODO: For HBASE Master we implemented get_specific_hbase_master.
        # TODO: We might need to do that for other ha components too
        """
        :return: A dict {host: ha_state, host: ha_state, ...}
        """
        url = '{0}/services/{1}/components/{2}?fields=' \
              'host_components/HostRoles/ha_state'.format(self.clustername, service, component_name)
        response = self.request_ambari(url).json()
        result = {}
        for host in response['host_components']:
            result[host['HostRoles']['host_name']] = host['HostRoles']['ha_state']
        return result

    def get_specific_nn_host(self, state='active'):
        """
        :return: Hostname of the requested namenode (active or standby)
        """
        nn_info = self.get_component_info('HDFS', 'NAMENODE')

        for host in nn_info['host_components']:
            state_info = self.request_ambari("{0}?fields=metrics/dfs/FSNamesystem/HAState".format(host['href'])).json()
            if state_info['metrics']['dfs']['FSNamesystem']['HAState'] == state:
                return state_info['HostRoles']['host_name']

    def get_specific_hbase_master(self, state='active'):
        isActiveMaster = 'true'
        if state is not 'active':
            isActiveMaster = 'false'
        master_info = self.get_component_info('HBASE', 'HBASE_MASTER')

        for host in master_info['host_components']:
            state_info = self.request_ambari(
                "{0}?fields=metrics/hbase/master/IsActiveMaster".format(host['href'])).json()
            if state_info['metrics']['hbase']['master']['IsActiveMaster'] == isActiveMaster:
                return state_info['HostRoles']['host_name']

    def get_component_state(self, service, component_name):
        self.logger.info("Getting component state from Ambari API {0}/{1}".format(service, component_name))
        url = '{0}/services/{1}/components/{2}?fields=' \
              'ServiceComponentInfo/total_count,' \
              'ServiceComponentInfo/started_count,' \
              'ServiceComponentInfo/state'.format(self.clustername, service, component_name)
        response = self.request_ambari(url).json()
        self.logger.debug(response)
        return response

    def check_if_all_components_started(self, service, component_name):
        state_info = self.get_component_state(service, component_name)
        return self._check_if_all_components_started(state_info)

    def get_random_host_and_component_path(self, info):
        host_component_info = random.choice(info['host_components'])
        return host_component_info['HostRoles']['host_name'], host_component_info['href']

    def change_host_component_state_and_wait(self, path, state='STARTED', polling_timeout=None):
        polling_timeout = self.polling_timeout if polling_timeout is None else polling_timeout
        self.logger.info("Change state for {path} to state {state}".format(path=path, state=state))
        response = self._change_host_component_state(path, state)
        try:
            response_json = response.json()
            return self._wait_for_request_completion(response_json['href'], polling_timeout=polling_timeout)
        except json.decoder.JSONDecodeError:
            # If component wasn't in stopped state response returned is empty but still 200 OK
            self.logger.warning("Component wasn't in stopped state response returned is empty but still 200 OK")
            pass

    def get_service_info(self, service):
        self.logger.info("Getting component info from Ambari API {0}".format(service))
        url = '{0}/services/{1}'.format(self.clustername, service)
        response = self.request_ambari(url).json()
        self.logger.debug(response)
        return response

    def get_sample_hosts(self, info, k=10):
        host_component_info = random.sample(info['host_components'], k)
        hosts = []
        for info in host_component_info:
            hosts.append(info['HostRoles']['host_name'])
        return hosts
