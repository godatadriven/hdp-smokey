class VerificationError(Exception):
    pass


class Verifier:
    """
    Verifier can be extended to accommodate specific verifying of components under test

    The SmokeTest class uses the verify() method to check if the functionality of the component under test is still
    functioning ok. The verify method should raise a VerificationError if something is wrong.
    """

    def __init__(self, logger):
        self.logger = logger

    def verify(self):
        """
        Default implementation that always raises a VerificationError

        :return: a VerificationError if something is wrong
        """
        self.logger.info("Verify functionality")
        raise VerificationError("Error verifying functionality")


