import threading

_success = 0
_forbidden_403 = 0
_lock = threading.Lock()


def inc_success():
    global _success
    with _lock:
        _success += 1


def inc_403():
    global _forbidden_403
    with _lock:
        _forbidden_403 += 1


def pop_minute_report():
    global _success, _forbidden_403
    with _lock:
        report = {"success": _success, "forbidden_403": _forbidden_403}
        _success = 0
        _forbidden_403 = 0
        return report
