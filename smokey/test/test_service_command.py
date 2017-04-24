import unittest
from unittest.mock import patch, MagicMock
import logging

import service.command as service_command


class TestCommand(unittest.TestCase):
    def setUp(self):
        self.command = service_command.Command('localhost', logging)

    @patch('ssh.client.ssh.sendCommand')
    def test_stop_process(self, cmd_mock):
        cmd_mock.return_value = ([], 0, [])
        status = self.command.call_service_action('test', 'stop')
        self.assertEqual(status, 0)

    @patch('ssh.client.ssh.sendCommand')
    def test_stop_process_with_fail(self, cmd_mock):
        cmd_mock.return_value = (['Stop failed in mock testing process'], 42, ['stderr text'])
        with self.assertRaises(service_command.ServiceActionError):
            self.command.call_service_action('test', 'stop')

    @patch('ssh.client.ssh.sendCommand', MagicMock(return_value=([], 0, [])))
    def test_stop_process_creating_correct_command(self):
        self.command.call_service_action('test', 'stop')
        self.command.connection.sendCommand.assert_called_once_with('sudo service test stop')
