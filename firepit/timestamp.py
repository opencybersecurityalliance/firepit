from datetime import datetime

KNOWN_TIMESTAMPS = {
    'accessed',
    'created',
    'date',
    'end',
    'first_observed',
    'last_observed',
    'modified',
    'start',
    'timestamp',
}


TIME_FMT = '%Y-%m-%dT%H:%M:%S.%f'


def timefmt(t, prec=3):
    """Format Python datetime `t` in RFC 3339-format"""
    val = t.strftime(TIME_FMT)
    parts = val.split('.')
    if len(parts) > 1:
        l = len(parts[0])
        digits = parts[1]
        num_digits = len(digits)
        if num_digits:
            l += min(num_digits, prec) + 1
    return val[:l] + 'Z'


def to_datetime(timestamp):
    """Convert RFC 3339-format `timestamp` to Python datetime"""
    return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
