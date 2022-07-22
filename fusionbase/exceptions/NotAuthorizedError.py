class NotAuthorizedError(Exception):
    def __init__(
        self,
        type="authorization_error.missing",
        message="You are not authorized to access this resource.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
