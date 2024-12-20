"""Custom exceptions for the friendly fraud model."""


class APIError(ValueError):
    """Exception raised when an error occurs with data API requests."""

    def __init__(self, message="An error occurred with the data API request."):
        self.message = message
        super().__init__(self.message)


class MissingAPIKeyError(ValueError):
    """Exception raised when the API key is missing."""

    def __init__(
        self,
        message="You have not provided an API key in the body of your request. Please provide an API key in the following form before the 'parameters' block: {'authentication' : {'X-API-KEY': '<your_api_key>'}}",
    ):
        super().__init__(message)


class IncorrectAPIKeyError(ValueError):
    """Exception raised when the API key is incorrect."""

    def __init__(
        self,
        message="The API key you provided is incorrect. Please contact your system administrator.",
    ):
        super().__init__(message)
