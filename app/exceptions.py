"""Shared exceptions for all collectors."""


class SentinelError(Exception):
    """Base for all SENTINEL errors."""


class APIError(SentinelError):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class AuthenticationError(SentinelError):
    pass


class RateLimitError(SentinelError):
    def __init__(self, message: str = "Rate limit hit", retry_after: int = 60):
        super().__init__(message)
        self.retry_after = retry_after


class ScraperError(SentinelError):
    pass
