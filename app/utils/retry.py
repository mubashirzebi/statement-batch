import time


def sleep_with_backoff(base_seconds: float, attempt_number: int, max_delay: float = 60.0) -> None:
    delay = float(base_seconds) * (2 ** max(0, attempt_number - 1))
    time.sleep(min(delay, max_delay))
