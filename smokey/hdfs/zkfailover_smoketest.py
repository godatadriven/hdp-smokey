import core.base as base
import hdfs.hdfs_verifiers as hdfs_verifiers


class FailoverControllerSmokeTest(base.SmokeTest):
    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('HDFS', 'ZKFC', logname='smoketest-zkfc.log', stop_type=cmd_line_type,
                         process_user='hdfs', process_indicator='DFSZKFailoverController', verification_count=2)
        self.verifiers = [hdfs_verifiers.HdfsVerifier(logger=self.logger, filename='zkfc_smoketest_verifier_file.txt',
                                                      ambari=self.ambari),
                          hdfs_verifiers.HdfsMrVerifier(logger=self.logger, ambari=self.ambari),
                          hdfs_verifiers.HdfsSparkVerifier(logger=self.logger),
                          hdfs_verifiers.HdfsSparkVerifier(logger=self.logger, spark2=True)]

