import ssh.client


class ServiceActionError(Exception):
    pass


class Command:
    def __init__(self, host, logger):
        self.connection = ssh.client.ssh(host)
        self.logger = logger

    def call_service_action(self, service, action='status'):
        output, status, errors = self.connection.sendCommand("sudo service {0} {1}".format(service, action))
        if status != 0:
            raise ServiceActionError("Service call failed! Output was {0}, err was {1}".format(output, errors))

        return status
