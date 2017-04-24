import core.base as base

import hive.hive_verifier as hive_verifier


class HiveMetastoreSmokeTest(base.SmokeTest):
    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('HIVE', 'HIVE_METASTORE', logname='smoketest-hive.log', stop_type=cmd_line_type,
                         process_user='hive', process_indicator='org.apache.hadoop.hive.metastore.HiveMetaStore',
                         stop_realization_timeout=15)
        self.verifiers = hive_verifier.HiveVerifier(logger=self.logger, zk_nodes="sandbox:2181", database='foodmart',
                                                    query='select * from customer limit 10;', zookeeper=True)
