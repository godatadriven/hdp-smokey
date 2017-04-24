import ssh.client


class UnknownPidError(Exception):
    pass


class CommandError(Exception):
    pass


class Command:
    def __init__(self, host, logger):
        self.connection = ssh.client.ssh(host)
        self.logger = logger

    def get_pid_for_user_with_grep_expression(self, user, grep):
        pid = -1
        output, status, errors = self.connection.sendCommand("ps -u {0} -f | grep {1}".format(user, grep))
        if status == 0:
            pid = self.get_pid_from_ps_output(output)

        return pid

    def get_pid_from_ps_output(self, output):
        if type(output) is not list:
            self.logger.debug(
                "Command output was not of expected type list. Type was {0}, Content was '{1}'".format(type(output),
                                                                                                       output))
            raise UnknownPidError("Command output was not of expected type list.")

        if len(output) == 0:
            self.logger.debug("Command output was not of expected size. Size was {0}".format(len(output)))
            raise UnknownPidError("Command output was not of expected size. Expected at least 1 element.")

        columns = output[0].split()
        if len(columns) < 2:
            self.logger.debug(
                "Command output was not of expected size. Size was {0}. Content was '{1}'".format(len(columns),
                                                                                                  columns))
            raise UnknownPidError("Command output was not of expected size. Expected at least 2 elements.")

        return columns[1]

    def kill_process_of_user_with_pid(self, user, pid, signal='-TERM'):
        output, status, errors = self.connection.sendCommand("sudo -u {0} kill {1} {2}".format(user, signal, pid))
        self._check_exit_status(output, status, errors)
        return status

    def ls(self, path):
        output, status, errors = self.connection.sendCommand("ls {0}".format(path))
        self._check_exit_status(output, status, errors)

    def get_anaconda_python_version_info(self, python_major_version):
        output, status, errors = self.connection.sendCommand(
            "/hadoop/software/anaconda/anaconda{0}/bin/conda search python --offline --unknown".format(
                python_major_version))
        self._check_exit_status(output, status, errors)
        return output, status, errors

    @staticmethod
    def _check_exit_status(output, status, errors):
        if status != 0:
            raise CommandError("Command failed! Output was {0}, err was {1}".format(output, errors))
