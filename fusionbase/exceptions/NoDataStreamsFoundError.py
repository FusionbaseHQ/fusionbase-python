class NoDataStreamsFoundError(Exception):
    def __init__(
        self, type="data_warning.empty", message="We could not find any data streams."
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
