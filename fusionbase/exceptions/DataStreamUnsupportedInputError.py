class DataStreamUnsupportedInputError(Exception):
    def __init__(
        self,
        type="data_warning.empty",
        message="The input data you provided is not supported for creating a data stream.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
