import time


def sleep_with_backoff(base_seconds: float, attempt_number: int) -> None:
    delay = float(base_seconds) * (2 ** max(0, attempt_number - 1))
    time.sleep(delay)
