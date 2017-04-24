import unittest
from unittest.mock import Mock, patch

import ambari.api as api


class TestAmbariApi(unittest.TestCase):
    def setUp(self):
        self.ambari = api.Api(request_timeout=2, polling_timeout=5)

    def test_encode_escaped_json_string_for_ambari(self):
        data = {"foo": "bar", "baz": {"nr": 42}}
        self.assertRegex(self.ambari._encode_escaped_json_string_for_ambari(data), '\\\\"foo\\\\": \\\\"bar\\\\"')
        self.assertRegex(self.ambari._encode_escaped_json_string_for_ambari(data), '\\\\"baz\\\\": {\\\\"nr\\\\": 42}')

    @patch('requests.request')
    def test_mocking_request(self, mock_request):
        response = {'ServiceComponentInfo': {'component_name': 'KRIS_GATEWAY', 'display_name': 'Kris Gateway',
                                             'unknown_count': 0, 'state': 'STARTED', 'service_name': 'KRIS',
                                             'total_count': 2, 'cluster_name': 'Sandbox', 'recovery_enabled': 'false',
                                             'init_count': 0, 'installed_count': 0, 'category': 'MASTER',
                                             'started_count': 3, 'install_failed_count': 0}, 'host_components': [{
            'HostRoles': {
                'component_name': 'KRIS_GATEWAY',
                'cluster_name': 'Sandbox',
                'host_name': 'sandbox.hortonworks.com'},
            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'},
            {
                'HostRoles': {
                    'component_name': 'KRIS_GATEWAY',
                    'cluster_name': 'Sandbox',
                    'host_name': 'sandbox.hortonworks.com'},
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'}],
                    'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/services/KRIS/components/KRIS_GATEWAY'}

        mock_request.return_value = Mock(ok=True, status_code=200, cookies={'AMBARISESSIONID': 'KRISUUID12345678'})
        mock_request.return_value.json.return_value = response

        ambari_response = self.ambari.get_component_info('KNOX', 'KNOX_GATEWAY')
        self.assertEqual(response, ambari_response)

    @patch('requests.request')
    def test_check_response(self, mock_request):
        response = {'MockedResponse': {'Mock': True}}

        mock_request.return_value = Mock(ok=False, status_code=403, cookies={'AMBARISESSIONID': 'KRISUUID12345678'})
        mock_request.return_value.json.return_value = response

        with self.assertRaises(api.AmbariRequestError):
            self.ambari.get_component_info('KNOX', 'KNOX_GATEWAY')

    @patch('requests.request')
    def test_ambari_session(self, mock_request):
        response = {'MockedResponse': {'Mock': True}}

        mock_request.return_value = Mock(ok=True, status_code=200, cookies={'AMBARISESSIONID': 'KRISUUID12345678'})
        mock_request.return_value.json.return_value = response

        ambari_response = self.ambari.get_component_info('KNOX', 'KNOX_GATEWAY')
        self.assertEqual(response, ambari_response)
        self.assertEqual('KRISUUID12345678', self.ambari.ambari_session)
        self.assertDictEqual({
            "Content-Type": "application/json",
            "X-Requested-By": "smoketest",
            "Cookie": "AMBARISESSIONID=KRISUUID12345678"
        }, self.ambari.default_ambari_headers)

    def test_check_components_started(self):
        info = {'ServiceComponentInfo': {'component_name': 'KRIS_GATEWAY', 'display_name': 'Kris Gateway',
                                         'unknown_count': 0, 'state': 'STARTED', 'service_name': 'KRIS',
                                         'total_count': 2, 'cluster_name': 'Sandbox', 'recovery_enabled': 'false',
                                         'init_count': 0, 'installed_count': 0, 'category': 'MASTER',
                                         'started_count': 2, 'install_failed_count': 0}, 'host_components': [{
            'HostRoles': {
                'component_name': 'KRIS_GATEWAY',
                'cluster_name': 'Sandbox',
                'host_name': 'sandbox.hortonworks.com'},
            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'},
            {
                'HostRoles': {
                    'component_name': 'KRIS_GATEWAY',
                    'cluster_name': 'Sandbox',
                    'host_name': 'sandbox.hortonworks.com'},
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'}],
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/services/KRIS/components/KRIS_GATEWAY'}
        result = self.ambari._check_if_all_components_started(info)
        self.assertTrue(result)

    def test_check_components_not_started_state(self):
        info = {'ServiceComponentInfo': {'component_name': 'KRIS_GATEWAY', 'display_name': 'Kris Gateway',
                                         'unknown_count': 0, 'state': 'INSTALLED', 'service_name': 'KRIS',
                                         'total_count': 2, 'cluster_name': 'Sandbox', 'recovery_enabled': 'false',
                                         'init_count': 0, 'installed_count': 0, 'category': 'MASTER',
                                         'started_count': 3, 'install_failed_count': 0}, 'host_components': [{
            'HostRoles': {
                'component_name': 'KRIS_GATEWAY',
                'cluster_name': 'Sandbox',
                'host_name': 'sandbox.hortonworks.com'},
            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'},
            {
                'HostRoles': {
                    'component_name': 'KRIS_GATEWAY',
                    'cluster_name': 'Sandbox',
                    'host_name': 'sandbox.hortonworks.com'},
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'}],
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/services/KRIS/components/KRIS_GATEWAY'}
        result = self.ambari._check_if_all_components_started(info)
        self.assertFalse(result)

    def test_check_components_not_started_nr_of_services(self):
        info = {'ServiceComponentInfo': {'component_name': 'KRIS_GATEWAY', 'display_name': 'Kris Gateway',
                                         'unknown_count': 0, 'state': 'STARTED', 'service_name': 'KRIS',
                                         'total_count': 2, 'cluster_name': 'Sandbox', 'recovery_enabled': 'false',
                                         'init_count': 0, 'installed_count': 1, 'category': 'MASTER',
                                         'started_count': 1, 'install_failed_count': 0}, 'host_components': [{
            'HostRoles': {
                'component_name': 'KRIS_GATEWAY',
                'cluster_name': 'Sandbox',
                'host_name': 'sandbox.hortonworks.com'},
            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'},
            {
                'HostRoles': {
                    'component_name': 'KRIS_GATEWAY',
                    'cluster_name': 'Sandbox',
                    'host_name': 'sandbox.hortonworks.com'},
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'}],
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/services/KRIS/components/KRIS_GATEWAY'}
        result = self.ambari._check_if_all_components_started(info)
        self.assertFalse(result)

    def test_get_random_host_and_component_path(self):
        info = {'ServiceComponentInfo': {'component_name': 'KRIS_GATEWAY', 'display_name': 'Kris Gateway',
                                         'unknown_count': 0, 'state': 'STARTED', 'service_name': 'KRIS',
                                         'total_count': 2, 'cluster_name': 'Sandbox', 'recovery_enabled': 'false',
                                         'init_count': 0, 'installed_count': 0, 'category': 'MASTER',
                                         'started_count': 2, 'install_failed_count': 0}, 'host_components': [{
            'HostRoles': {
                'component_name': 'KRIS_GATEWAY',
                'cluster_name': 'Sandbox',
                'host_name': 'sandbox.hortonworks.com'},
            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'},
            {
                'HostRoles': {
                    'component_name': 'KRIS_GATEWAY',
                    'cluster_name': 'Sandbox',
                    'host_name': 'sandbox.hortonworks.com'},
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'}],
                'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/services/KRIS/components/KRIS_GATEWAY'}
        host, path = self.ambari.get_random_host_and_component_path(info)
        self.assertIn(host, ['sandbox.hortonworks.com', 'sandbox.hortonworks.com'])
        self.assertIn(path, [
            'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY',
            'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'])
        self.assertEqual(path,
                         'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/' + host + '/host_components/KRIS_GATEWAY')

    @patch('requests.request')
    def test_timeout_waiting_for_service_state_change(self, mock_request):
        response = {'tasks': [
            {'Tasks': {'stage_id': 0, 'id': 46910, 'cluster_name': 'Sandbox', 'request_id': 10045},
             'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/requests/10045/tasks/46910'}
        ],
            'Requests': {
                'request_schedule': None,
                'task_count': 1,
                'progress_percent': 35.0,
                'failed_task_count': 0,
                'operation_level': None,
                'type': 'INTERNAL_REQUEST',
                'request_context': 'Change state of https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KNOX_GATEWAY via ambari_api.py (REST)',
                'aborted_task_count': 0,
                'id': 10045,
                'completed_task_count': 0,
                'resource_filters': [],
                'exclusive': False,
                'cluster_name': 'Sandbox',
                'timed_out_task_count': 0,
                'end_time': -1,
                'request_status': 'IN_PROGRESS',
                'create_time': 1485187338349,
                'queued_task_count': 0,
                'start_time': 1485187338360,
                'inputs': None
            },
            'stages': [
                {'Stage': {'stage_id': 0,
                           'cluster_name': 'Sandbox',
                           'request_id': 10045
                           },
                 'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/requests/10045/stages/0'
                 }
            ],
            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/requests/10045'
        }

        mock_request.return_value = Mock(ok=True, status_code=200, cookies={'AMBARISESSIONID': 'KRISUUID12345678'})
        mock_request.return_value.json.return_value = response

        with self.assertRaises(api.AmbariWaitForCompletionTimeoutError):
            self.ambari._wait_for_request_completion(
                'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/requests/10045')

    def test_get_sample_hosts(self):
        info = {'ServiceComponentInfo': {'component_name': 'KRIS_GATEWAY', 'display_name': 'Kris Gateway',
                                         'unknown_count': 0, 'state': 'STARTED', 'service_name': 'KRIS',
                                         'total_count': 2, 'cluster_name': 'Sandbox', 'recovery_enabled': 'false',
                                         'init_count': 0, 'installed_count': 0, 'category': 'MASTER',
                                         'started_count': 2, 'install_failed_count': 0}, 'host_components': [{
                                         'HostRoles': {
                                            'component_name': 'KRIS_GATEWAY',
                                            'host_name': 'sandbox.hortonworks.com'},
                                            'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'
                                         },
                                         { 'HostRoles': {
                                                  'component_name': 'KRIS_GATEWAY',
                                                  'cluster_name': 'Sandbox',
                                                  'host_name': 'sandbox.hortonworks.com'},
                                                  'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/hosts/sandbox.hortonworks.com/host_components/KRIS_GATEWAY'}],
                                                  'href': 'https://sandbox.hortonworks.com:8443/api/v1/clusters/Sandbox/services/KRIS/components/KRIS_GATEWAY'
                                         }
        hosts = self.ambari.get_sample_hosts(info, k=1)
        for host in hosts:
            self.assertIn(host, ['sandbox.hortonworks.com', 'sandbox.hortonworks.com'])
