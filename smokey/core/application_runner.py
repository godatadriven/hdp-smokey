import logging
import subprocess
import os


class ApplicationRequestError(Exception):
    pass


class ApplicationRunner:
    def __init__(self, application_timeout=60, logger=logging):
        self.timeout = application_timeout
        self.logger = logger
        self.application_args = self._get_application_args()

    def _get_application_args(self):
        return ()

    def _get_application_env(self):
        return {**os.environ, "HDP_VERSION": "2.5.0.0-1245"}

    def run(self):
        try:
            app = subprocess.run(
                self.application_args,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.timeout,
                env=self._get_application_env())
            self._check_run_status(app)
            self._check_output(app.stdout, app.stderr)
        except subprocess.TimeoutExpired as toe:
            self.logger.error("TimeoutException occurred. StdOut was {0}".format(toe.stdout))
            self.logger.error("TimeoutException occurred. StdErr was {0}".format(toe.stderr))
            raise ApplicationRequestError(toe)

    def _check_run_status(self, process):
        if process.returncode != 0:
            self.logger.error(
                "Command [{0}] returned non zero exit code [{1}]. Output: {2}; Stderr: {3}".format(
                    process.args, process.returncode, process.stdout, process.stderr))
            raise ApplicationRequestError("Application returned non zero exit code")

    def _check_output(self, output, err):
        self.logger.error("Method not implemented in base class")
        raise ApplicationRequestError("Output checking not implemented in base class.")


class SparkApplicationRunner(ApplicationRunner):
    def __init__(self, application_timeout=60, logger=logging, spark2=False):

        if spark2:
            self.client_dir = "spark2-client"
            self.jar_location = "examples/jars"
            self.spark_env = {"SPARK_MAJOR_VERSION": "2"}
        else:
            self.client_dir = "spark-client"
            self.jar_location = "lib"
            self.spark_env = {"SPARK_MAJOR_VERSION": "1"}

        super().__init__(application_timeout=application_timeout, logger=logger)

    def _get_application_env(self):
        return {**super()._get_application_env(), **self.spark_env}


class HiveApplicationRunner(ApplicationRunner):
    def __init__(self, application_timeout=60, logger=logging, principal='', database='', server='localhost',
                 zk_nodes='', port=10001, query='show tables;', zookeeper=False):
        if zookeeper:
            self.url = "jdbc:hive2://{0}/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2".format(
                zk_nodes)
        else:
            self.url = "jdbc:hive2://{0}:{1}/default;transportMode=http;httpPath=cliservice;principal={2}".format(
                server, port, principal)
        self.query = query
        self.database = database
        super().__init__(application_timeout=application_timeout, logger=logger)


class MrApplicationRunner(ApplicationRunner):
    def __init__(self, logger=logging):
        super().__init__(application_timeout=120, logger=logger)
