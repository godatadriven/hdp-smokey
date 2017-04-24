import logging
import re
import sys

import core.application_runner as runner


class HiveRequestError(runner.ApplicationRequestError):
    pass


class HiveRunner(runner.HiveApplicationRunner):
    def _get_application_args(self):
        return "beeline", "-u", self.url, "-e", "use {0}".format(self.database), "-e", self.query

    def _check_output(self, output, err):
        match = re.search(r'Connected to: Apache Hive', err.decode(), re.MULTILINE)
        match_no_connection = re.search(r'No current connection', err.decode(), re.MULTILINE)
        if match is None or match_no_connection is not None:
            self.logger.error("Requesting hive resulted in the following error: {0}".format(err))
            self.logger.error("Requesting hive resulted in the following output: {0}".format(output))
            raise HiveRequestError("Requesting hive resulted in an error.")
