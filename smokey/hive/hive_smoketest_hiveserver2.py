import argparse

import core.base as base

import hive.hive_verifier as hive_verifier


class HiveServerSmokeTest(base.SmokeTest):

    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('HIVE', 'HIVE_SERVER', logname='smoketest-hive-hs2.log', stop_type=cmd_line_type, process_user='hive', process_indicator='org.apache.hive.component.server.HiveServer2', stop_realization_timeout=15)
        self.query='select * from customer limit 10;'
        self.verifiers = hive_verifier.HiveVerifier(logger=self.logger, zk_nodes="sandbox:2181", database='foodmart', query=self.query, zookeeper=True)



