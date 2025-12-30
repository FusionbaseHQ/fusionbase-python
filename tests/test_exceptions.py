"""Tests for Fusionbase exception classes and error handling."""

import unittest
from unittest.mock import MagicMock

from fusionbase.exceptions import APIError
from fusionbase.exceptions import AuthenticationError
from fusionbase.exceptions import AuthorizationError
from fusionbase.exceptions import FusionbaseError
from fusionbase.exceptions import handle_http_error
from fusionbase.exceptions import InvalidArgumentError
from fusionbase.exceptions import parse_error_response
from fusionbase.exceptions import RateLimitError
from fusionbase.exceptions import RequestValidationError
from fusionbase.exceptions import ResourceNotFoundError
from fusionbase.exceptions import ServerError
from fusionbase.exceptions import ValidationError


class TestFusionbaseError(unittest.TestCase):
    """Test cases for base FusionbaseError."""

    def test_fusionbase_error_with_message(self):
        """Test creating error with message only."""
        error = FusionbaseError("Something went wrong")

        self.assertEqual(error.message, "Something went wrong")
        self.assertIsNone(error.status_code)
        self.assertEqual(str(error), "Something went wrong")

    def test_fusionbase_error_with_status_code(self):
        """Test creating error with message and status code."""
        error = FusionbaseError("Server error", status_code=500)

        self.assertEqual(error.message, "Server error")
        self.assertEqual(error.status_code, 500)

    def test_fusionbase_error_is_exception(self):
        """Test that FusionbaseError is an Exception."""
        error = FusionbaseError("Test error")

        self.assertIsInstance(error, Exception)

    def test_fusionbase_error_can_be_raised(self):
        """Test that FusionbaseError can be raised and caught."""
        with self.assertRaises(FusionbaseError) as context:
            raise FusionbaseError("Test raise", status_code=400)

        self.assertEqual(context.exception.message, "Test raise")
        self.assertEqual(context.exception.status_code, 400)


class TestInvalidArgumentError(unittest.TestCase):
    """Test cases for InvalidArgumentError."""

    def test_invalid_argument_error(self):
        """Test InvalidArgumentError creation."""
        error = InvalidArgumentError("Invalid parameter: foo")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Invalid parameter: foo")


class TestResourceNotFoundError(unittest.TestCase):
    """Test cases for ResourceNotFoundError."""

    def test_resource_not_found_with_type_and_id(self):
        """Test ResourceNotFoundError with resource type and ID."""
        error = ResourceNotFoundError(
            resource_type="organization",
            resource_id="org_123"
        )

        self.assertEqual(error.resource_type, "organization")
        self.assertEqual(error.resource_id, "org_123")
        self.assertEqual(error.status_code, 404)
        self.assertIn("Organization not found", error.message)
        self.assertIn("org_123", error.message)

    def test_resource_not_found_with_custom_message(self):
        """Test ResourceNotFoundError with custom message."""
        error = ResourceNotFoundError(message="Custom not found message")

        self.assertEqual(error.message, "Custom not found message")
        self.assertEqual(error.status_code, 404)

    def test_resource_not_found_default_message(self):
        """Test ResourceNotFoundError with default message."""
        error = ResourceNotFoundError()

        self.assertEqual(error.message, "Resource not found")
        self.assertEqual(error.status_code, 404)

    def test_resource_not_found_with_response(self):
        """Test ResourceNotFoundError with response object."""
        mock_response = MagicMock()
        error = ResourceNotFoundError(response=mock_response)

        self.assertEqual(error.response, mock_response)

    def test_resource_not_found_is_fusionbase_error(self):
        """Test ResourceNotFoundError inherits from FusionbaseError."""
        error = ResourceNotFoundError()

        self.assertIsInstance(error, FusionbaseError)


class TestAuthenticationError(unittest.TestCase):
    """Test cases for AuthenticationError."""

    def test_authentication_error(self):
        """Test AuthenticationError creation."""
        error = AuthenticationError("Invalid API key")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Invalid API key")


class TestAuthorizationError(unittest.TestCase):
    """Test cases for AuthorizationError."""

    def test_authorization_error(self):
        """Test AuthorizationError creation."""
        error = AuthorizationError("Access denied")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Access denied")


class TestRequestValidationError(unittest.TestCase):
    """Test cases for RequestValidationError."""

    def test_request_validation_error(self):
        """Test RequestValidationError creation."""
        error = RequestValidationError("Invalid request body")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Invalid request body")


class TestServerError(unittest.TestCase):
    """Test cases for ServerError."""

    def test_server_error(self):
        """Test ServerError creation."""
        error = ServerError("Internal server error")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Internal server error")


class TestRateLimitError(unittest.TestCase):
    """Test cases for RateLimitError."""

    def test_rate_limit_error(self):
        """Test RateLimitError creation."""
        error = RateLimitError("Too many requests")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Too many requests")


class TestAPIError(unittest.TestCase):
    """Test cases for APIError."""

    def test_api_error_basic(self):
        """Test APIError with basic parameters."""
        error = APIError("API call failed", status_code=400)

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "API call failed")
        self.assertEqual(error.status_code, 400)

    def test_api_error_with_request_id(self):
        """Test APIError with request_id."""
        error = APIError(
            "API call failed",
            status_code=500,
            request_id="req_abc123"
        )

        self.assertEqual(error.request_id, "req_abc123")

    def test_api_error_with_operation_id(self):
        """Test APIError with operation_id."""
        error = APIError(
            "API call failed",
            status_code=500,
            operation_id="op_xyz789"
        )

        self.assertEqual(error.operation_id, "op_xyz789")

    def test_api_error_with_response(self):
        """Test APIError with response object."""
        mock_response = MagicMock()
        error = APIError(
            "API call failed",
            status_code=500,
            response=mock_response
        )

        self.assertEqual(error.response, mock_response)

    def test_api_error_all_parameters(self):
        """Test APIError with all parameters."""
        mock_response = MagicMock()
        error = APIError(
            "Complete error",
            status_code=502,
            request_id="req_123",
            operation_id="op_456",
            response=mock_response
        )

        self.assertEqual(error.message, "Complete error")
        self.assertEqual(error.status_code, 502)
        self.assertEqual(error.request_id, "req_123")
        self.assertEqual(error.operation_id, "op_456")
        self.assertEqual(error.response, mock_response)


class TestValidationError(unittest.TestCase):
    """Test cases for ValidationError."""

    def test_validation_error(self):
        """Test ValidationError creation."""
        error = ValidationError("Field 'name' is required")

        self.assertIsInstance(error, FusionbaseError)
        self.assertEqual(error.message, "Field 'name' is required")
        self.assertEqual(error.status_code, 400)


class TestHandleHttpError(unittest.TestCase):
    """Test cases for handle_http_error function."""

    def test_handle_404_error(self):
        """Test handling 404 errors."""
        mock_error = MagicMock()
        mock_error.status_code = 404
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Not found"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, ResourceNotFoundError)

    def test_handle_401_error(self):
        """Test handling 401 errors."""
        mock_error = MagicMock()
        mock_error.status_code = 401
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Unauthorized"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, AuthenticationError)
        self.assertEqual(result.message, "Unauthorized")

    def test_handle_403_error(self):
        """Test handling 403 errors."""
        mock_error = MagicMock()
        mock_error.status_code = 403
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Forbidden"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, AuthorizationError)
        self.assertEqual(result.message, "Forbidden")

    def test_handle_422_error(self):
        """Test handling 422 errors."""
        mock_error = MagicMock()
        mock_error.status_code = 422
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Validation failed"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, RequestValidationError)
        self.assertEqual(result.message, "Validation failed")

    def test_handle_429_error(self):
        """Test handling 429 errors."""
        mock_error = MagicMock()
        mock_error.status_code = 429
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Rate limit exceeded"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, RateLimitError)
        self.assertEqual(result.message, "Rate limit exceeded")

    def test_handle_500_error(self):
        """Test handling 500 errors."""
        mock_error = MagicMock()
        mock_error.status_code = 500
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Server error"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, ServerError)
        self.assertEqual(result.message, "Server error")

    def test_handle_502_error(self):
        """Test handling 502 errors (also ServerError)."""
        mock_error = MagicMock()
        mock_error.status_code = 502
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {"message": "Bad gateway"}

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, ServerError)

    def test_handle_other_error(self):
        """Test handling other status codes."""
        mock_error = MagicMock()
        mock_error.status_code = 418  # I'm a teapot
        mock_error.response = MagicMock()
        mock_error.response.json.return_value = {
            "message": "I'm a teapot",
            "request_id": "req_123",
            "operation_id": "op_456"
        }

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, APIError)
        self.assertEqual(result.message, "I'm a teapot")
        self.assertEqual(result.status_code, 418)

    def test_handle_error_non_json_response(self):
        """Test handling error with non-JSON response."""
        mock_error = MagicMock()
        mock_error.status_code = 500
        mock_error.response = MagicMock()
        mock_error.response.json.side_effect = ValueError("No JSON")
        mock_error.response.text = "Plain text error"

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, ServerError)
        self.assertEqual(result.message, "Plain text error")

    def test_handle_error_no_response(self):
        """Test handling error with no response object."""
        mock_error = MagicMock()
        mock_error.status_code = 500
        mock_error.response = None

        result = handle_http_error(mock_error)

        self.assertIsInstance(result, ServerError)


class TestParseErrorResponse(unittest.TestCase):
    """Test cases for parse_error_response function."""

    def test_parse_404_response(self):
        """Test parsing 404 response."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {"message": "Not found"}

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, ResourceNotFoundError)

    def test_parse_401_response(self):
        """Test parsing 401 response."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"message": "Invalid token"}

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, AuthenticationError)
        self.assertEqual(result.message, "Invalid token")

    def test_parse_403_response(self):
        """Test parsing 403 response."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.json.return_value = {"message": "Access denied"}

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, AuthorizationError)

    def test_parse_422_response(self):
        """Test parsing 422 response."""
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = {"message": "Invalid input"}

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, RequestValidationError)

    def test_parse_429_response(self):
        """Test parsing 429 response."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.json.return_value = {"message": "Too many requests"}

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, RateLimitError)

    def test_parse_500_response(self):
        """Test parsing 500 response."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"message": "Internal error"}

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, ServerError)

    def test_parse_other_response(self):
        """Test parsing other status codes."""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "message": "Bad request",
            "request_id": "req_abc",
            "operation_id": "op_xyz"
        }

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, APIError)
        self.assertEqual(result.message, "Bad request")
        self.assertEqual(result.status_code, 400)

    def test_parse_non_json_response(self):
        """Test parsing response with non-JSON body."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.side_effect = ValueError("No JSON")
        mock_response.text = "Error text"

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, ServerError)
        self.assertEqual(result.message, "Error text")

    def test_parse_response_with_parsing_error(self):
        """Test parsing response when parsing fails."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.side_effect = Exception("Unexpected error")

        result = parse_error_response(mock_response)

        self.assertIsInstance(result, APIError)
        self.assertIn("Error parsing response", result.message)


class TestExceptionInheritance(unittest.TestCase):
    """Test exception inheritance relationships."""

    def test_all_exceptions_inherit_from_base(self):
        """Test all custom exceptions inherit from FusionbaseError."""
        exceptions = [
            InvalidArgumentError("test"),
            ResourceNotFoundError(),
            AuthenticationError("test"),
            AuthorizationError("test"),
            RequestValidationError("test"),
            ServerError("test"),
            RateLimitError("test"),
            APIError("test"),
            ValidationError("test"),
        ]

        for exc in exceptions:
            self.assertIsInstance(
                exc, FusionbaseError,
                f"{type(exc).__name__} should inherit from FusionbaseError"
            )

    def test_exceptions_can_be_caught_by_base(self):
        """Test catching all exceptions by base class."""
        exceptions_to_raise = [
            InvalidArgumentError("test"),
            ResourceNotFoundError(),
            AuthenticationError("test"),
            ServerError("test"),
        ]

        for exc in exceptions_to_raise:
            try:
                raise exc
            except FusionbaseError:
                pass  # Expected
            except Exception:
                self.fail(f"{type(exc).__name__} was not caught by FusionbaseError")
