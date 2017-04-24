import unittest
from unittest.mock import patch, MagicMock
import logging

import command.command as command


class TestCommand(unittest.TestCase):
    def setUp(self):
        self.command = command.Command('localhost', logging)

    def test_get_pid_from_ps_output(self):
        output = ["user pid parentpid 7 date time the rest"]
        self.assertEqual(self.command.get_pid_from_ps_output(output), 'pid')

    def test_get_pid_from_ps_output_with_empty_output(self):
        output = []
        with self.assertRaises(command.UnknownPidError):
            self.command.get_pid_from_ps_output(output)

    def test_get_pid_from_ps_output_with_wrong_output(self):
        output = ["blahblahblah"]
        with self.assertRaises(command.UnknownPidError):
            self.command.get_pid_from_ps_output(output)

    def test_get_pid_from_ps_output_with_wrong_output_type(self):
        output = "blahblahblah"
        with self.assertRaises(command.UnknownPidError):
            self.command.get_pid_from_ps_output(output)

    @patch('ssh.client.ssh.sendCommand')
    def test_kill_process_with_pid(self, cmd_mock):
        cmd_mock.return_value = ([], 0, [])
        status = self.command.kill_process_of_user_with_pid('test', 123456789)
        self.assertEqual(status, 0)

    @patch('ssh.client.ssh.sendCommand')
    def test_kill_process_with_pid_fail(self, cmd_mock):
        cmd_mock.return_value = (['Kill failed in mock testing process'], 42, ['stderr text'])
        with self.assertRaises(command.CommandError):
            self.command.kill_process_of_user_with_pid('test', 123456789)

    @patch('ssh.client.ssh.sendCommand', MagicMock(return_value=([], 0, [])))
    def test_kill_process_creating_correct_command(self):
        self.command.kill_process_of_user_with_pid('test', 123456789)
        self.command.connection.sendCommand.assert_called_once_with('sudo -u test kill -TERM 123456789')

    def test_check_exit_status_ok(self):
        output = []
        status = 0
        errors = []
        self.command._check_exit_status(output, status, errors)

    def test_check_exit_status_nok(self):
        output = ['help']
        status = 2
        errors = ['something went wrong']
        with self.assertRaises(command.CommandError):
            self.command._check_exit_status(output, status, errors)
