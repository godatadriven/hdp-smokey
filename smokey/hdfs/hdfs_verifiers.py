import core.verification as verification
import hdfs.hdfs_api as hdfs_api
import hdfs.hdfs_application_runner as runner


class HdfsVerifier(verification.Verifier):
    def __init__(self, logger, filename='hdfs_verifier_file.txt', filesize=324, ambari=None):
        super().__init__(logger)
        self.ambari = ambari
        self.filename = "/user/smoketest/hdfs_smoketest/{0}".format(filename)
        self.filesize=filesize

    def verify(self):
        self.logger.info("Verify HDFS status by writing a file and checking the md5sum")
        active_nn = self.ambari.get_specific_nn_host(state='active')
        hdfs = hdfs_api.HdfsApi(logger=self.logger, active_nn_host=active_nn)
        md5 = hdfs.create_hdfs_file_of_size_in_mb(self.filename, size=self.filesize)
        remote_md5 = hdfs.get_hdfsfile_and_calc_md5(self.filename)
        hdfs.cleanup_remote_file(self.filename)
        if md5 != remote_md5:
            raise verification.VerificationError(
                "local md5 {0} did not match remote md5 {1} for file {2}".format(md5, remote_md5, self.filename))


class HdfsDatanodeVerifier(verification.Verifier):
    def __init__(self, logger, active_nn_host=None):
        super().__init__(logger)
        if active_nn_host is not None:
            self.hdfs = hdfs_api.HdfsApi(logger=self.logger, active_nn_host=active_nn_host)
        else:
            self.hdfs = hdfs_api.HdfsApi(logger=self.logger)

        self.filename = "/user/smoketest/hdfs_smoketest/hdfs_dn_test_file_with_known_md5.txt"
        self.md5 = "<put your md5 here>"

    def verify(self):
        self.logger.info("Verify HDFS status by reading a file and checking the md5sum")
        remote_md5 = self.hdfs.get_hdfsfile_and_calc_md5(self.filename)
        if self.md5 != remote_md5:
            raise verification.VerificationError(
                "known md5 {0} did not match remote md5 {1} for file {2}".format(self.md5, remote_md5, self.filename))


class HdfsMrVerifier(verification.Verifier):
    def __init__(self, logger, ambari=None):
        super().__init__(logger)
        self.ambari = ambari
        self.teragen = runner.MrTeragenRunner(logger=self.logger)
        self.terasort = runner.MrTerasortRunner(logger=self.logger)

    def verify(self):
        self.logger.info("Verify Mapreduce with teragen/terasort")
        try:
            active_nn = self.ambari.get_specific_nn_host(state='active')
            hdfs = hdfs_api.HdfsApi(logger=self.logger, active_nn_host=active_nn)
            hdfs.cleanup_remote_file("/user/smoketest/hdfs_smoketest/teragenout", recursive=True)
            hdfs.cleanup_remote_file("/user/smoketest/hdfs_smoketest/terasortout", recursive=True)
            self.teragen.run()
            self.terasort.run()
        except runner.MrRequestError as e:
            self.logger.error("MapReduce Hdfs test teragen/terasort runner status NOT OK!!")
            raise verification.VerificationError(e)


class HdfsSparkVerifier(verification.Verifier):
    def __init__(self, logger, spark2=False):
        super().__init__(logger)
        self.runner = runner.SparkHdfsTestRunner(logger=self.logger, spark2=spark2)

    def verify(self):
        self.logger.info("Verify Spark Hdfs with HdfsTest")
        try:
            self.runner.run()
        except runner.SparkRequestError as e:
            self.logger.error("Spark HdfsTest runner status NOT OK!!")
            raise verification.VerificationError(e)


