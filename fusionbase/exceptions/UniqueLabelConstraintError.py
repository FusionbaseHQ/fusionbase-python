class UniqueLabelConstraintError(Exception):
    def __init__(
        self, type="data_warning.error", message="Provided unique label already exists."
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
