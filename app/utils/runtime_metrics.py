import os

try:
    import psutil
except ImportError:  # pragma: no cover - optional until dependency is installed
    psutil = None


_PROCESS = psutil.Process(os.getpid()) if psutil else None
if _PROCESS:
    _PROCESS.cpu_percent(interval=None)


def current_metrics():
    if not _PROCESS:
        return {"memory_mb": "", "cpu_percent": ""}

    memory_mb = _PROCESS.memory_info().rss / (1024 * 1024)
    cpu_percent = _PROCESS.cpu_percent(interval=None)
    return {
        "memory_mb": "%.1f" % memory_mb,
        "cpu_percent": "%.1f" % cpu_percent,
    }
