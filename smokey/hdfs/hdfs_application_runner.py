import re
import glob

import core.application_runner as runner


class SparkRequestError(runner.ApplicationRequestError):
    pass


class MrRequestError(runner.ApplicationRequestError):
    pass


class SparkHdfsTestRunner(runner.SparkApplicationRunner):
    def _get_application_args(self):
        example_jars = glob.glob(
            "/usr/hdp/current/{0}/{1}/spark-examples*.jar".format(self.client_dir, self.jar_location))
        if len(example_jars) > 0:
            jar = example_jars[0]
        else:
            jar = "/usr/hdp/current/{0}/{1}/spark-examples.jar".format(self.client_dir, self.jar_location)

        return ("spark-submit",
                "--class", "org.apache.spark.examples.HdfsTest", "--master", "yarn-client",
                "--num-executors", "3", "--driver-memory", "512m", "--executor-memory", "512m", "--executor-cores", "1",
                jar,
                "/user/smoketest/hdfs_smoketest/SparkHdfsTestInputFile.txt")

    def _check_output(self, output, err):
        match = re.findall(r'^Iteration\s+\d+\s+took\s+\d+\s+ms\s*$', output.decode(), re.MULTILINE)
        if match is None or len(match) != 10:
            self.logger.error("HdfsTest with spark-submit returned wrong output. Output: {0}".format(output))
            raise SparkRequestError("HdfsTest with spark-submit returned wrong output.")


class MrTeragenRunner(runner.MrApplicationRunner):
    @staticmethod
    def _get_application_args():
        return ("/usr/hdp/current/hadoop-yarn-client/bin/yarn",
                "jar", "/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar", "teragen",
                "500000",
                "/user/smoketest/hdfs_smoketest/teragenout")

    def _check_output(self, output, err):
        match_err = re.search(r'Job job_\d+_\d+ completed successfully', err.decode())
        match_out = re.search(r'Job job_\d+_\d+ completed successfully', output.decode())
        if match_err is None and match_out is None:
            self.logger.error("Teragen with mr-submit returned wrong output. Output: {0}".format(output))
            raise MrRequestError("Teragen with mr-submit returned wrong output.")


class MrTerasortRunner(runner.MrApplicationRunner):
    @staticmethod
    def _get_application_args():
        return ("/usr/hdp/current/hadoop-yarn-client/bin/yarn",
                "jar", "/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar", "terasort",
                "/user/smoketest/hdfs_smoketest/teragenout", "/user/smoketest/hdfs_smoketest/terasortout")

    def _check_output(self, output, err):
        match_err = re.search(r'Job job_\d+_\d+ completed successfully', err.decode())
        match_out = re.search(r'Job job_\d+_\d+ completed successfully', output.decode())
        if match_err is None and match_out is None:
            self.logger.error("Terasort with mr-submit returned wrong output. Output: {0}".format(output))
            raise MrRequestError("Terasort with mr-submit returned wrong output.")
