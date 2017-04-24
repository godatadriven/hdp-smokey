import re
import glob

import core.application_runner as runner


class SparkRequestError(runner.ApplicationRequestError):
    pass


class SparkPiRunner(runner.SparkApplicationRunner):
    def _get_application_args(self):
        example_jars = glob.glob(
            "/usr/hdp/current/{0}/{1}/spark-examples*.jar".format(self.client_dir, self.jar_location))
        if len(example_jars) > 0:
            jar = example_jars[0]
        else:
            jar = "/usr/hdp/current/{0}/{1}/spark-examples.jar".format(self.client_dir, self.jar_location)

        return ("/usr/hdp/current/{0}/bin/spark-submit".format(self.client_dir),
                "--class", "org.apache.spark.examples.SparkPi", "--master", "yarn-client",
                "--num-executors", "3", "--driver-memory", "512m", "--executor-memory", "512m", "--executor-cores", "1",
                jar, "100")

    def _check_output(self, output, err):
        match = re.search(r'^Pi is roughly 3.14\d+$', output.decode(), re.MULTILINE)
        if match is None:
            self.logger.error("Calculating PI with spark-submit returned wrong output. Output: {0}".format(output))
            raise SparkRequestError("Calculating PI with spark-submit returned wrong output.")
