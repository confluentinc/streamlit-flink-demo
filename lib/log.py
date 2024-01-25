from datetime import datetime
from queue import SimpleQueue, Empty
from collections import OrderedDict
import coloredlogs
import inspect
import logging
import os
import traceback
import uuid


_debug = False
_level = None
_fmt = '%(asctime)s %(levelname)s %(name)s %(message)s'
_date_fmt = '%H:%M:%S'
_subscriptions = None
_silence_loggers = (
  # urllib3's debug logger is very noisy, just keep it at WARNING
  'urllib3',
  'asyncio',
  # No idea what this thing even is but it can log unhelpful output
  'charset_normalizer'
)


class LogEventsSubscription(SimpleQueue):

  def __init__(self):
    self.id = uuid.uuid4()

  def __iter__(self):
    return self

  def __next__(self):
    return self.get()

  def get(self, block=True, timeout=None):
    try:
      return super().get(block=block, timeout=timeout)
    except Empty:
      # For convenience, return None on empty or timeout
      pass


# Our Logger class doesn't currently do anything special, but it's
# useful to differentiate between our loggers and all of the other
# loggers being managed by the logging framework
class EventLogger(logging.Logger):
  pass


class EventAdapter(logging.LoggerAdapter):

  def process(self, msg, kw):
    # The super's process() method overwrites the extra argument passed to
    # a logging call. We use this argument to capture arbitrary events when
    # logging, so we keep it around by making this method a noop.
    return msg, kw

  def log(self, level, msg, *args, extra=None, **kw):
    # The contents of the extra argument are used to add attributes to
    # the log record that will eventually be created. We use this to pass
    # through arbitrary events given by the caller when logging. If the
    # caller has supplied an extra argument, it will be assigned to the
    # _event attribute of the log record being created.
    if extra:
      extra = {
        '_event': extra
      }
    super().log(level, msg, *args, extra=extra, **kw)

  def exception(self, msg, *args, extra=None, exc_info=True, **kw):
    lines = traceback.format_exc().split('\n')
    event = {
      'traceback': lines
    }
    if extra:
      event.update(extra)

    # This will ultimately call into the log() method above, so no
    # need to place extra under the '_event' key yet
    super().exception(str(msg), *args, extra=event, exc_info=exc_info, **kw)


# Shim that intercepts all logging events, appending each raw event
# with its level to the events queue for downstream consumption
class EventHandler(logging.Handler):

  def __init__(self):
    logging.Handler.__init__(self=self)

  def emit(self, record):
    # In addition to logging arbitrary structured events, caller can also
    # override certain fields of the logging output. Check for any of these
    # overrides and update the log record accordingly.
    extra = {}
    if hasattr(record, '_event'):
      extra = record._event
    if extra:
      record.name = extra.pop('_name', record.name)
      record.levelname = extra.pop('_level', record.levelname)

    # We use OrderedDict here so that event fields are rendered downstream
    # in a predictable order
    event = OrderedDict({
      'name': record.name,
      'level': record.levelname.strip(),
      'timestamp': datetime.fromtimestamp(record.created),
      'message': self.formatter.format(record),
    })

    # If the originally logged record includes an event, update our event
    # to include its properties.
    if extra:
      event.update(extra)
    # Write a copy of the event into each subscriber queue. If we don't have
    # any subscribers yet, this is a noop.
    if _subscriptions:
      for sub in _subscriptions.values():
        sub.put(event)


def subscribe():
  global _subscriptions
  # This may not be initialized if caller hasn't already called get_logger
  if _subscriptions is None:
    _subscriptions = {}

  sub = LogEventsSubscription()
  _subscriptions[sub.id] = sub
  return _subscriptions[sub.id]


def set_debug(debug):
  global _level
  global _debug
  _debug = debug
  _level = logging.DEBUG if _debug else logging.INFO
  # If anyone is calling get_logger globally within a module, their loggers won't reflect
  # debug mode if it's being set at execution time (e.g. via command-line argument),
  # which comes after module load time. We just reconfigure here to account for this so
  # that callers aren't forced to call get_logger within a narrower scope like a function
  # body. Real programmers only use global variables anyways.
  for log in logging.root.manager.loggerDict.values():
    # Only change level for our loggers
    if isinstance(log, EventLogger):
      log.setLevel(_level)
      coloredlogs.install(
        logger=log, fmt=_fmt, datefmt=_date_fmt,
        level=logging.getLevelName(_level)
      )


def get_logger():
  global _events
  global _level
  global _subscriptions
  # _level will never be None after we perform this one-time initialization
  if _level is None:
    set_debug(_debug)
    # Silence noisy loggers we don't need
    for name in _silence_loggers:
      logging.getLogger(name).setLevel(logging.WARNING)
    # Try to determine if the caller is attempting to run flinklab without activating
    # a virtual environment, which probably isn't going to be a good time. We want to
    # catch this scenario regardless of what modules the caller is using, and everything
    # uses logs so this is a good place to check. We also only want to log this warning
    # once on startup. All around great option if you ask me. Great option.
    if 'VIRTUAL_ENV' not in os.environ:
      get_logger().warning('virtual environment has not been activated')
    coloredlogs.install(
      fmt=_fmt, datefmt=_date_fmt,
      level=logging.getLevelName(_level)
    )
    _subscriptions = {}

  # Get the filename of the caller's module, which we use as our logger name
  caller_path = inspect.stack()[1].filename
  _, name = os.path.split(caller_path)
  # Note that getLogger will always return the same Logger reference for a given
  # name, so there is no need to cache this here. It will only be instantiated once.
  logging.setLoggerClass(EventLogger)
  log = logging.getLogger(name)

  # We only need to add handlers once
  if not log.handlers:
    eh = EventHandler()
    eh.setFormatter(logging.Formatter(_fmt, _date_fmt))
    log.addHandler(eh)

  # We wrap our loggers in an adapter that acts as a shim intercepting all log events.
  # We use this to support logging of arbitrary events, without changing the signatures
  # of any of the core logging interfaces.
  #
  # We could just as easily do this by adding the event handling functionality to a
  # custom Logger class, but we use an adapter because all log messages are passed
  # through a single log() method when using adapters. Thus we don't have to override
  # all methods corresponding to each log level (e.g. info(), warning(), etc.). We
  # just override the adapter's process() method.
  log = EventAdapter(log)
  log.setLevel(_level)

  return log
