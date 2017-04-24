import core.base as base
import hdfs.hdfs_api as hdfs_api
import hdfs.hdfs_verifiers as hdfs_verifiers


class DatanodeSmokeTest(base.SmokeTest):
    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('HDFS', 'DATANODE', logname='smoketest-dn.log', stop_type=cmd_line_type,
                         process_user='hdfs', process_indicator='SecureDataNodeStarter', stop_realization_timeout=15)
        active_nn = self.ambari.get_specific_nn_host(state='active')
        self.hdfs = hdfs_api.HdfsApi(logger=self.logger, active_nn_host=active_nn)
        self.hdfs_filename = "/user/smoketest/hdfs_smoketest/hdfs_dn_test_file_with_known_md5.txt"
        self.verifiers = hdfs_verifiers.HdfsDatanodeVerifier(logger=self.logger, active_nn_host=active_nn)

    def _get_random_host_and_component_path(self):
        """
        Get a specific datanode host and path from AMBARI

        :return: (hostname, component_path)
        """
        info = self.ambari.get_component_info(self.service, self.component)
        datanode_host = self.hdfs.get_host_location_of_first_block(self.hdfs_filename)
        info['host_components'] = [host for host in info['host_components'] if
                                   host['HostRoles']['host_name'] == datanode_host]

        return self.ambari.get_random_host_and_component_path(info)

