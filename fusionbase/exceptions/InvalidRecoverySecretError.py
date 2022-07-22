class InvalidRecoverySecretError(Exception):
    def __init__(
        self,
        type="data_warning.error",
        message="The secret that you provided is invalid.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
