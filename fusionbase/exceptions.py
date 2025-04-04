"""Exception module for Fusionbase SDK."""


# Base exceptions
class FusionbaseError(Exception):
    """Base exception for Fusionbase SDK."""


class InvalidArgumentError(FusionbaseError):
    """Exception raised for invalid arguments."""


class ResourceNotFoundError(FusionbaseError):
    """Exception raised when a resource is not found."""

    def __init__(self,
                 resource_type=None,
                 resource_id=None,
                 response=None,
                 message=None):
        """Initialize ResourceNotFoundError.

        Args:
            resource_type: Type of resource not found (e.g., "location")
            resource_id: ID of the resource that wasn't found
            response: The HTTP response that triggered this error
            message: Custom error message
        """
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.response = response
        self.status_code = 404

        if message:
            self.message = message
        elif resource_type and resource_id:
            self.message = f"{resource_type.capitalize()} not found (ID: {resource_id})"
        else:
            self.message = "Resource not found"

        super().__init__(self.message)


class AuthenticationError(FusionbaseError):
    """Exception raised for authentication issues."""


class AuthorizationError(FusionbaseError):
    """Exception raised when user lacks permissions for an operation."""


class RequestValidationError(FusionbaseError):
    """Exception raised when request validation fails."""


class ServerError(FusionbaseError):
    """Exception raised for server errors."""


class RateLimitError(FusionbaseError):
    """Exception raised when rate limit is exceeded."""


class APIError(FusionbaseError):
    """Exception raised for API errors."""

    def __init__(
        self,
        message,
        status_code=None,
        request_id=None,
        operation_id=None,
        response=None,
    ):
        """Initialize API error.

        Args:
            message: The error message
            status_code: The HTTP status code
            request_id: The request ID
            operation_id: The operation ID
            response: The raw response
        """
        self.status_code = status_code
        self.request_id = request_id
        self.operation_id = operation_id
        self.response = response
        super().__init__(message)


def handle_http_error(error):
    """Handle HTTP errors from the Fusionbase API.

    Args:
        error: HTTP error

    Returns:
        A specific Fusionbase exception
    """
    status_code = getattr(error, "status_code", 500)
    try:
        response = getattr(error, "response", None)
        if response:
            # Try to parse response JSON
            try:
                error_data = response.json()
                message = error_data.get("message", str(error))
                request_id = error_data.get("request_id")
                operation_id = error_data.get("operation_id")
            except ValueError:
                # If not JSON, use response text
                message = response.text or str(error)
                request_id = None
                operation_id = None
        else:
            message = str(error)
            request_id = None
            operation_id = None
    except (AttributeError, ValueError) as e:  # Catch specific exceptions
        message = f"Error handling HTTP error: {str(e)}. Original error: {str(error)}"
        request_id = None
        operation_id = None

    # Return appropriate error based on status code
    if status_code == 404:
        return ResourceNotFoundError(response=response)
    if status_code == 401:
        return AuthenticationError(message)
    if status_code == 403:
        return AuthorizationError(message)
    if status_code == 422:
        return RequestValidationError(message)
    if status_code == 429:
        return RateLimitError(message)
    if status_code >= 500:
        return ServerError(message)

    # Default case
    return APIError(message, status_code, request_id, operation_id)


def parse_error_response(response):
    """Parse an error response and return the appropriate exception.

    Args:
        response: HTTP response object

    Returns:
        A specific Fusionbase exception
    """
    status_code = getattr(response, "status_code", 500)

    try:
        # Try to parse response JSON
        try:
            error_data = response.json()
            message = error_data.get("message", str(response))
            request_id = error_data.get("request_id")
            operation_id = error_data.get("operation_id")
        except (ValueError, AttributeError):
            # If not JSON, use response text
            message = getattr(response, "text", str(response)) or str(response)
            request_id = None
            operation_id = None

        # Return appropriate error based on status code
        if status_code == 404:
            return ResourceNotFoundError(response=response)
        if status_code == 401:
            return AuthenticationError(message)
        if status_code == 403:
            return AuthorizationError(message)
        if status_code == 422:
            return RequestValidationError(message)
        if status_code == 429:
            return RateLimitError(message)
        if status_code >= 500:
            return ServerError(message)

        # Default case
        return APIError(message,
                        status_code,
                        request_id,
                        operation_id,
                        response=response)

    except Exception as e:  # pylint: disable=bare-except
        # Fallback in case of any parsing errors
        return APIError(
            f"Error parsing response: {str(e)}. Status code: {status_code}",
            status_code=status_code,
            response=response)
