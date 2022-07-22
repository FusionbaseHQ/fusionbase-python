class InvalidParameterError(Exception):
    def __init__(
        self,
        type="value_error.invalid",
        message="Some parameters you provided are invalid.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
