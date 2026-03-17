from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception

from delta_exchange_bot.api.delta_client import DeltaAPIError


def _is_retryable_exception(exc: Exception) -> bool:
    if isinstance(exc, DeltaAPIError):
        message = str(exc).lower()
        non_retryable_markers = (
            "bad_schema",
            "validation_error",
            "insufficient_margin",
            "signature mismatch",
            "ip_not_whitelisted",
            "negative_order_size",
            "unauthorized",
            "forbidden",
            "http 400",
            "http 401",
            "http 403",
            "http 404",
        )
        if any(marker in message for marker in non_retryable_markers):
            return False
        retryable_markers = ("http 429", "http 500", "http 502", "http 503", "http 504", "timeout")
        return any(marker in message for marker in retryable_markers)
    return True


def retry_on_exception():
    return retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(4),
        retry=retry_if_exception(_is_retryable_exception),
    )
