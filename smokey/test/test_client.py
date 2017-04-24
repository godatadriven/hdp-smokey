import unittest
from unittest.mock import MagicMock, patch
import io
import paramiko

import ssh.client as ssh_client


class TestSsh(unittest.TestCase):
    @patch('paramiko.client.SSHClient', MagicMock)
    def setUp(self):
        self.ssh = ssh_client.ssh('localhost')

    @patch('getpass.getpass')
    def test_get_password(self, mock_pass):
        mock_pass.return_value = 'bleh'

        passwd = self.ssh.getPassword()
        self.assertEqual(passwd, 'bleh')

    def test_get_password_from_env(self):
        with patch.dict('ssh.client.os.environ', {'AMBARI_PWD': 'blih'}):
            passwd = self.ssh.getPassword()
            self.assertEqual(passwd, 'blih')

    def test_connection_called(self):
        with patch.dict('ssh.client.os.environ', {'AMBARI_PWD': 'blah'}):
            self.ssh.initConnection()
            self.ssh.client.connect.assert_called_once_with('localhost', look_for_keys=False, password='blah',
                                                            username='smoketest')

    def test_connection_called_on_sending_first_command(self):
        with patch.dict('ssh.client.os.environ', {'AMBARI_PWD': 'blah'}):
            # Prepare test
            stdout = MagicMock(spec=io.IOBase, channel=MagicMock(spec=paramiko.channel.Channel))
            stderr = MagicMock(spec=io.IOBase, channel=MagicMock(spec=paramiko.channel.Channel))
            stdin = MagicMock(spec=io.IOBase, channel=MagicMock(spec=paramiko.channel.Channel))

            stdout.readlines.return_value = ['You are kris!']
            stdout.channel.recv_exit_status.return_value = 21
            stderr.readlines.return_value = ['Error occurred on stderr']
            self.ssh.client.exec_command.return_value = (stdin, stdout, stderr)

            # Test
            output, status, errors = self.ssh.sendCommand('whoami')

            # Assertions
            self.ssh.client.connect.assert_called_once_with('localhost', look_for_keys=False, password='blah',
                                                            username='smoketest')
            self.ssh.client.exec_command.assert_called_once_with('whoami')
            self.ssh.client.close.assert_called_with()
            self.assertEqual(status, 21)
            self.assertEqual(output[0], 'You are kris!')
            self.assertEqual(errors[0], 'Error occurred on stderr')

    def test_connection_called_only_once_on_sending_two_commands(self):
        with patch.dict('ssh.client.os.environ', {'AMBARI_PWD': 'blah'}):
            # Prepare test
            stdout = MagicMock(spec=io.IOBase, channel=MagicMock(spec=paramiko.channel.Channel))
            stderr = MagicMock(spec=io.IOBase, channel=MagicMock(spec=paramiko.channel.Channel))
            stdin = MagicMock(spec=io.IOBase, channel=MagicMock(spec=paramiko.channel.Channel))

            stdout.readlines.return_value = ['You are kris!', 'Sitting next to pearl!']
            stdout.channel.recv_exit_status.return_value = 42
            stderr.readlines.return_value = []
            self.ssh.client.exec_command.return_value = (stdin, stdout, stderr)

            # Test
            output, status, errors = self.ssh.sendCommand('whoami')

            # Assertions
            self.ssh.client.connect.assert_called_with('localhost', look_for_keys=False, password='blah',
                                                       username='smoketest')
            self.ssh.client.exec_command.assert_called_with('whoami')
            self.ssh.client.close.assert_called_with()
            self.assertEqual(status, 42)
            self.assertEqual(output[0], 'You are kris!')

            output2, status2, errors2 = self.ssh.sendCommand('whoami2')
            # Assertions
            self.ssh.client.connect.assert_called_with('localhost', look_for_keys=False, password='blah',
                                                       username='smoketest')
            self.ssh.client.exec_command.assert_called_with('whoami2')
            self.ssh.client.close.assert_called_with()
            self.assertEqual(status2, 42)
            self.assertEqual(output2[1], 'Sitting next to pearl!')
