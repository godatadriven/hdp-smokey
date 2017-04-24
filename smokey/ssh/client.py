import os
import getpass
import sys
import logging

from paramiko import client


class ssh:
    client = None

    def __init__(self, address, logger=logging):
        self.address = address
        self.username = os.environ.get('AMBARI_USER', 'smoketest')
        self.logger = logger

        self.client = client.SSHClient()
        self.client.set_missing_host_key_policy(client.AutoAddPolicy())

    def getPassword(self):
        passwd = os.environ.get('AMBARI_PWD')
        if passwd is None:
            passwd = getpass.getpass(stream=sys.stderr)
        return passwd

    def initConnection(self):
        self.logger.info("Connecting to server.")
        password = self.getPassword()
        self.client.connect(self.address, username=self.username, password=password, look_for_keys=False)

    def sendCommand(self, command):
        self.initConnection()
        status = -1
        out = []
        err = []
        if self.client:
            self.logger.info("Executing command")
            self.logger.debug("Executing command {0}".format(command))
            stdin, stdout, stderr = self.client.exec_command(command)

            status = stdout.channel.recv_exit_status()
            self.logger.debug("Command exited with status {0}".format(status))
            stdin.flush()
            stdin.channel.shutdown_write()

            self.client.close()
            out = stdout.readlines()

            if status != 0:
                err = stderr.readlines()
            self.logger.debug("Command had output: {0}. And possibly some errors: {1}".format(out, err))
        else:
            print("Connection not opened.")
            self.logger.error("SSH Connection was not open!!")
        return out, status, err
