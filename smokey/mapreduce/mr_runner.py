import re
import core.application_runner as runner


class MrRequestError(runner.ApplicationRequestError):
    pass


class MrPiRunner(runner.MrApplicationRunner):
    @staticmethod
    def _get_application_args():
        return ("/usr/hdp/current/hadoop-yarn-client/bin/yarn",
                "jar", "/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar", "pi",
                "8", "1000")

    def _check_output(self, output, err):
        match = re.search(r'^Estimated value of Pi is 3.14\d+$', output.decode(), re.MULTILINE)
        if match is None:
            self.logger.error("Calculating PI with mr-submit returned wrong output. Output: {0}".format(output))
            raise MrRequestError("Calculating PI with mr-submit returned wrong output.")
