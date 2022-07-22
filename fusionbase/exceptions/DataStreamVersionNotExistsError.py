class DataStreamVersionNotExistsError(Exception):
    def __init__(
        self,
        type="data_warning.empty",
        message="The data version you provided does not exist",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
