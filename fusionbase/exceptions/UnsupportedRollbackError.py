class UnsupportedRollbackError(Exception):
    def __init__(
        self,
        type="data_warning.error",
        message="Cannot rollback to this data version due to schema change, "
        "store update or because it's the most data recent "
        "version.",
    ):
        self.type = type
        self.message = message
        super().__init__(self.message)
