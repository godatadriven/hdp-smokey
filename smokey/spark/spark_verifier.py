import core.verification as verification

import spark.spark_runner as spark


class SparkVerifier(verification.Verifier):

    def __init__(self, logger, spark2=False):
        super().__init__(logger)
        self.runner = spark.SparkPiRunner(logger=self.logger, spark2=spark2)

    def verify(self):
        self.logger.info("Verify Spark status by calculating pi")
        try:
            self.runner.run()
        except spark.SparkRequestError as e:
            self.logger.error("Spark runner status NOT OK!!")
            raise verification.VerificationError(e)
