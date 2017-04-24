import time
from hdfs.hdfs_verifiers import HdfsVerifier
from hdfs.hdfs_api import HdfsApi
from core import base


class NamenodeFailoverVerifier(HdfsVerifier):
    def __init__(self, logger, filename='hdfs_verifier_file.txt', filesize=324, ambari=None):
        super().__init__(logger)
        self.ambari = ambari
        self.service = "HDFS"
        self.component = "NAMENODE"

    def verify(self):
        self.logger.info("Verify HDFS status by writing a file and checking the md5sum")
        active_nn = self.ambari.get_specific_nn_host(state='active')
        host, component_path = self.get_component_path(active_nn)
        self.logger.info("Stopping the {0} on {1}".format(self.component, host))
        self.ambari.change_host_component_state_and_wait(component_path, state='INSTALLED')
        time.sleep(120)
        new_active_nn = self.ambari.get_specific_nn_host(state='active')
        hdfs = HdfsApi(logger=self.logger, active_nn_host=new_active_nn)
        md5 = hdfs.create_hdfs_file_of_size_in_mb(self.filename, size=self.filesize)
        remote_md5 = hdfs.get_hdfsfile_and_calc_md5(self.filename)
        hdfs.cleanup_remote_file(self.filename)
        if md5 != remote_md5:
            raise base.VerificationError(
                "local md5 {0} did not match remote md5 {1} for file {2}".format(md5, remote_md5, self.filename))
        self.logger.info("Starting the previously stopped {0}".format(self.component))
        self.ambari.change_host_component_state_and_wait(component_path, state='STARTED')
        self.logger.info("Started the {0}".format(self.component))

    def get_component_path(self, active_nn):
        """
        Get a specific datanode host and path from AMBARI

        :return: (hostname, component_path)
        """
        info = self.ambari.get_component_info(self.service, self.component)
        info['host_components'] = [host for host in info['host_components'] if
                                   host['HostRoles']['host_name'] == active_nn]

        return self.ambari.get_random_host_and_component_path(info)
