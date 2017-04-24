import core.base as base
import core.verification as verification
import mapreduce.mr_runner as mr


class MapreduceVerifier(verification.Verifier):

    def __init__(self, logger):
        super().__init__(logger)
        self.runner = mr.MrPiRunner(logger=self.logger)

    def verify(self):
        self.logger.info("Verify Mapreduce status by calculating pi")
        try:
            self.runner.run()
        except mr.MrRequestError as e:
            self.logger.error("Mapreduce runner status NOT OK!!")
            raise verification.VerificationError(e)


class MrSmokeTest(base.SmokeTest):

    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__('MAPREDUCE2', 'HISTORYSERVER', logname='smoketest-mr.log', stop_type=cmd_line_type, process_user='mapred', process_indicator='JobHistoryServer', stop_realization_timeout=15)
        self.verifiers = MapreduceVerifier(logger=self.logger)

