class ServerError(Exception):
    def __init__(
        self,
        type="value_error.all",
        message="Sorry, something went wrong. Please try again later.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
