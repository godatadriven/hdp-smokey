import core.base as base
from hdfs.hdfs_verifiers import HdfsVerifier, HdfsMrVerifier, HdfsSparkVerifier
from hive.hive_verifier import HiveVerifier
from yarn.yarn_resourcemanager_smoketest import YarnResourceManagerVerifier


class ZookeeperSmokeTest(base.SmokeTest):
    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('ZOOKEEPER', 'ZOOKEEPER_SERVER', logname='smoketest-zookeeper.log', stop_type=cmd_line_type,
                         process_user='zookeeper', verification_count=1,
                         process_indicator='org.apache.zookeeper.server.quorum.QuorumPeerMain')
        self.query = 'select * from customer limit 10;'
        self.verifiers = [HiveVerifier(logger=self.logger, zk_nodes="sandbox:2181",
                                       database='foodmart', query=self.query, zookeeper=True),
                          HdfsVerifier(logger=self.logger, filename='nn_smoketest_verifier_file.txt',
                                       ambari=self.ambari),
                          HdfsMrVerifier(logger=self.logger, ambari=self.ambari),
                          HdfsSparkVerifier(logger=self.logger),
                          HdfsSparkVerifier(logger=self.logger, spark2=True),
                          YarnResourceManagerVerifier(logger=self.logger)
                          ]
