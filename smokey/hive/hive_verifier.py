import core.verification as verification
import hive.hive_runner as hive
import core.application_runner as runner


class HiveVerifier(verification.Verifier):
    def __init__(self, logger, principal='', database='', server='localhost', port=10001, query='show tables;',
                 zk_nodes='', zookeeper=False):
        super().__init__(logger)
        self.runner = hive.HiveRunner(logger=self.logger, principal=principal, database=database, server=server,
                                      port=port, query=query, zk_nodes=zk_nodes, zookeeper=zookeeper)

    def verify(self):
        self.logger.info("Verify Hive by running select query")
        try:
            self.runner.run()
        except runner.ApplicationRequestError as e:
            self.logger.error("Hive runner status NOT OK!!")
            raise verification.VerificationError(e)
