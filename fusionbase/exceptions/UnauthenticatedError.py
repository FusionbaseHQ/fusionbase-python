class UnauthenticatedError(Exception):
    def __init__(
        self,
        type="authentication_error.missing",
        message="Could not validate credentials.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
