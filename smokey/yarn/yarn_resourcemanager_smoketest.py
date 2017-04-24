import core.base as base
import core.verification as verification

import yarn.yarn as yarn


class YarnResourceManagerVerifier(verification.Verifier):

    def __init__(self, logger):
        super().__init__(logger)
        self.yarn = yarn.Yarn(logger=self.logger)

    def verify(self):
        self.logger.info("Verify YARN ResourceManager status")
        try:
            self.yarn.check_yarn_resourcemanager_status()
        except yarn.YarnRequestError as e:
            self.logger.error("YARN ResourceManager status NOT OK!")
            raise verification.VerificationError(e)


class YarnResourceManagerSmokeTest(base.SmokeTest):

    def __init__(self, cmd_line_type='AMBARI'):
        super().__init__(
            'YARN',
            'RESOURCEMANAGER',
            logname='smoketest-yarn-resourcemanager.log',
            stop_type=cmd_line_type,
            process_user='yarn',
            process_indicator='resourcemanager'
        )
        self.verifiers = YarnResourceManagerVerifier(logger=self.logger)
