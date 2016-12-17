from asyncio.tasks import ensure_future
from contextlib import contextmanager

from . import adv_validate_arguments


SIGNALS = {}

IGNORE_KEYS = None


def connect(signal_keys, callback):
    """Connect `callback` to signal's callback sequence.

    Example::

        def user_signup(user):
            ...
        connect("user_signup", user_signup)
    """
    for signal_key in split_signal_keys(signal_keys):
        if signal_key in SIGNALS:
            SIGNALS[signal_key].append(callback)
        else:
            SIGNALS[signal_key] = [callback]


def split_signal_keys(signal_keys):
    """ helper function to convert list or space-separated string to the list of signal names"""
    if isinstance(signal_keys, str):
        signal_keys = [signal_keys]
    ret = []
    for record in signal_keys:
        ret += [r.strip() for r in record.split()]
    return ret


def get_callbacks(signal_key):
    """
    Return the list of callbacks by signal key
    :param signal_key:
    :return: list of callables
    """
    return SIGNALS.get(signal_key, [])


def connect_deco(*signal_keys):
    """A decorator that can be used instead of using connect.

    Example::

        @connect_deco('user_deleted')
        def user_deleted(user_id):
            ...
    """
    def wrap(fn):
        connect(signal_keys, fn)
        return fn
    return wrap


async def send_signal(signal_keys, *args, **kwargs):
    """Sending a signal will iterate over a signal's callback.

    :param signal_keys: a string with space-separated list of signals to send
    :param _ignore_errors: if set to True, suppress exceptions on error handling
    :param \*args: signal arguments
    :param \*\*kwargs: signal keyword arguments

    Example::

        user = User(1)
        send_signal('user_signup', user=user)
    """
    for signal_key in signal_keys.split(' '):
        if IGNORE_KEYS and signal_key in IGNORE_KEYS:
            continue

        ignore_error = kwargs.pop('_ignore_errors', False)
        callbacks = get_callbacks(signal_key)

        for callback in callbacks:
            try:
                arguments, keyword_arguments = adv_validate_arguments(
                    callback, args, kwargs)

                await callback(*arguments, **keyword_arguments)
            except:
                if not ignore_error:
                    raise


@contextmanager
def ignore_keys_ctx(keys_to_ignore):
    global IGNORE_KEYS

    old_keys = IGNORE_KEYS
    IGNORE_KEYS = keys_to_ignore
    yield
    IGNORE_KEYS = old_keys

