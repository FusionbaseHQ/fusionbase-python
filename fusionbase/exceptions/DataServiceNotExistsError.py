class DataServiceNotExistsError(Exception):
    def __init__(self, type="data_warning.empty", message="This data service does not exist."):
        self.type = type
        self.message = message
        super().__init__(self.message)
