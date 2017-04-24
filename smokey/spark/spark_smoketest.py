import core.base as base

import spark.spark_verifier as spark_verifier


class SparkSmokeTest(base.SmokeTest):
    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('SPARK', 'SPARK_JOBHISTORYSERVER', logname='smoketest-spark.log', stop_type=cmd_line_type,
                         process_user='spark', process_indicator='HistoryServer | grep -v spark2',
                         stop_realization_timeout=15)
        self.verifiers = spark_verifier.SparkVerifier(logger=self.logger, spark2=False)


class Spark2SmokeTest(base.SmokeTest):
    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('SPARK2', 'SPARK2_JOBHISTORYSERVER', logname='smoketest-spark2.log', stop_type=cmd_line_type,
                         process_user='spark', process_indicator='HistoryServer | grep spark2',
                         stop_realization_timeout=15)
        self.verifiers = spark_verifier.SparkVerifier(logger=self.logger, spark2=True)
