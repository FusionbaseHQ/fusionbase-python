import requests

from fusionbase.exceptions.DataStreamNotExistsError import DataStreamNotExistsError
from fusionbase.exceptions.DataStreamUnsupportedInputError import (
    DataStreamUnsupportedInputError,
)
from fusionbase.exceptions.DataStreamVersionNotExistsError import (
    DataStreamVersionNotExistsError,
)
from fusionbase.exceptions.InvalidParameterError import InvalidParameterError
from fusionbase.exceptions.InvalidRecoverySecretError import InvalidRecoverySecretError
from fusionbase.exceptions.NoDataStreamsFoundError import NoDataStreamsFoundError
from fusionbase.exceptions.NotAuthorizedError import NotAuthorizedError
from fusionbase.exceptions.ServerError import ServerError
from fusionbase.exceptions.UnauthenticatedError import UnauthenticatedError
from fusionbase.exceptions.UniqueLabelConstraintError import UniqueLabelConstraintError
from fusionbase.exceptions.UnsupportedRollbackError import UnsupportedRollbackError


class ResponseEvaluator:
    """
    This class is used to validate all Responses returned by the Fusionbase API. If something goes wrong the
    evaluate method should throw the according Error or Exception
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def evaluate(response: requests.models.Response) -> bool:
        """
        The evaluate method should be used to evaluate all kind of server reponses returned by th eFusionbase API
        :param response: The response object returned by the python requests package when calling the Fusionbase API
        :return: True if the Request exits with the HTTP status Code 200 otherwise an exception will be raised!
        """
        if not isinstance(response, requests.models.Response):
            raise ValueError(
                f"Response needs to be a valid request reponse object (instance of "
                f"requests.models.Response) but was of type {type(response)}"
            )

        if response.status_code == 200:
            return True

        content = response.json()["detail"]

        if content == "DATA_STREAM_NOT_FOUND":
            raise DataStreamNotExistsError
        else:
            content = content[0]

        message = content["msg"]
        error_type = content["type"]

        def __raise_according_error():

            if (
                message == "Sorry, something went wrong. Please try again later."
                and error_type == "value_error.all"
            ):
                raise ServerError()

            elif (
                message == "This data stream does not exist."
                and error_type == "data_warning.empty"
            ):
                raise DataStreamNotExistsError()

            elif (
                message == "You are not authorized to access this resource."
                and error_type == "authorization_error" ".missing"
            ):
                raise NotAuthorizedError()

            elif (
                message == "We could not find any data streams."
                and error_type == "data_warning.empty"
            ):
                raise NoDataStreamsFoundError()

            elif (
                message == "The data version you provided does not exist."
                and error_type == "data_warning.empty"
            ):
                raise DataStreamVersionNotExistsError()

            elif (
                message
                == "The input data you provided is not supported for creating a data stream."
                and error_type == "data_warning.empty"
            ):
                raise DataStreamUnsupportedInputError()

            elif (
                message == "A data stream with the given unique label already exists."
                and error_type == "data_warning.error"
            ):
                raise UniqueLabelConstraintError(
                    message="A data stream with the given unique label already exists."
                )

            elif (
                message
                == "Cannot rollback to this data version due to schema change, store update or because it's "
                "the most data recent version."
                and error_type == "data_warning.error"
            ):
                raise UnsupportedRollbackError()

            elif (
                message == "The secret that you provided is invalid."
                and error_type == "data_warning.error"
            ):
                raise InvalidRecoverySecretError()

            elif (
                message == "Could not validate credentials."
                and error_type == "authentication_error.missing"
            ):
                raise UnauthenticatedError()

            elif (
                message == "The sort parameters you provided are invalid."
                and error_type == "value_error.invalid"
            ):
                raise InvalidParameterError(
                    message="The sort parameters you provided are invalid."
                )

            else:
                raise NotImplementedError(
                    "The according Exception for the error returned by the Fusionbase API hasn't been implemented "
                    "please raise an Issue on GITHUB"
                )

        __raise_according_error()
