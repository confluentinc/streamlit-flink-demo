import re


def parse_drop(m):
  target = m.group(1)
  # Not going to need backticks from here, this DROP statement
  # is going to Kafka and SR
  target = target.replace('`', '')
  return ('drop', target)

def parse_reset(m):
  # A RESET is rewritten as a SET without a value. The first group
  # will be everything after RESET, second group is the string within
  # quotes, or None if no argument was provided.
  return ('set', m.group(2))

def parse_set(m):
  g = m.groups()
  # If it's a bare SET without a value (i.e. SET 'key'),
  # we interpret it is setting a boolean to True
  if len(g) == 1:
    g = tuple(list(g) + [True])
  return ('set', g)

def parse_use_catalog(m):
  rewrite = ('sql.current-catalog', m.group(1))
  return ('set', rewrite)

def parse_use_database(m):
  rewrite = ('sql.current-database', m.group(1))
  return ('set', rewrite)

def parse_show(m):
  if m.groups():
    # Show a specific property
    return ('show', m.group(1))
  # Show all in-context properties
  return ('show', None)


# Regexes to parse simple expressions and a handler function to call for matches
# Order matters
HANDLERS = (
  # DROP TABLE <table>
  (re.compile(r'\s*DROP\s+TABLE\s+(.+)\s*', re.IGNORECASE), parse_drop),
  # Note that we allow SET and RESET on unquoted properties. Otherwise an unquoted SET
  # statement will be interpreted as a SQL statement and sent to the API. It's
  # too easy to accidentially create statements this way, so we just allow them.
  #
  # RESET [property]
  (re.compile(r'\s*RESET(\s*\'?([^\']+)?\'?)?', re.IGNORECASE), parse_reset),
  # SET 'key' = 'value'
  (re.compile(r'\s*SET\s+\'?([^\']+)\'?\s+(?:=|TO)\s+\'?([^\']+)\'?', re.IGNORECASE), parse_set),
  # SET 'key'
  (re.compile(r'\s*SET\s+\'?([^\']+)\'?', re.IGNORECASE), parse_set),
  # USE CATALOG <catalog>
  (re.compile(r'\s*USE\s+CATALOG\s+(.+)', re.IGNORECASE), parse_use_catalog),
  # Use <database>
  (re.compile(r'\s*USE\s+(.+)', re.IGNORECASE), parse_use_database),
  # SHOW 'key'
  (re.compile(r'\s*SHOW\s+\'?([^\']+)\'', re.IGNORECASE), parse_show),
  # SHOW
  (re.compile(r'\s*SHOW\s*$', re.IGNORECASE), parse_show),
)

# Comments just get stripped out, no parsing/handlers needed
STRIP_COMMENTS = (
  re.compile(r'\s*\-\-.*[$\n]*'),
  re.compile(r'/\*[^*]*\*+(?:[^/*][^*]*\*+)*/'),
)


class SqlScript(object):

  def __init__(self, properties=None, raw=None, path=None):
    if raw and path:
      raise ValueError('raw and path cannot both be set')
    self.parsed = []
    self.raw = []
    # Dictionary for any properties SET by whatever executes this script
    self.properties = properties or {}
    # As a matter of convenience, a path can be passed to the constructor,
    # in which case we automatically parse its contents
    if raw:
      if isinstance(raw, list):
        raw = '\n'.join(raw)
      self.write(raw)
      self.parse()
    if path:
      self.read(path)
      self.parse()

  def __iter__(self):
    return (p for p in self.parsed)

  def __len__(self):
    return len(self.parsed)

  def clear(self):
    self.raw = []
    self.parsed = []

  def read(self, path):
    with open(path) as f:
      raw = f.read()
      self.write(raw)

  def write(self, s):
    self.clear()
    for regex in STRIP_COMMENTS:
      s = regex.sub('', s)
    raw = [l.strip() for l in s.split(';') if l.strip()]
    self.raw = self.raw + raw
    self.parse()

  def parse(self):
    # We do super light parsing on .sql scripts to account for statements
    # that are not supported by Flink yet.
    #
    # We take all individual SQL statements (by splitting on ';'), and figure
    # out which ones needed to be handled outside of Flink. Currently these
    # are DROP TABLE and SET.
    #
    # The output of this simple parser is a properly ordered list of statements,
    # classified by whether or not they should be handled by Flink or elsewhere.
    statements = []
    for stmt in self.raw:
      parsed = None
      for regex, handler in HANDLERS:
        m = regex.match(stmt)
        if m:
          parsed = handler(m)
          statements.append(parsed)
          break
      if parsed:
        continue
      # It's not DROP, SET or USE so just treat it as a generic SQL statement
      # and hope for the best
      statements.append(('sql', stmt + ';'))
    self.parsed = statements

    return self.parsed

  def execute(self, handlers, begin_hook=None):
    if not self.parsed:
      self.parse()
    # The caller provides us with a mapping of each operation type to its
    # corresponding handler function
    for op, stmt in self:
      if op not in handlers:
        continue
      if begin_hook:
        begin_hook(op, stmt, self.properties)
      result = handlers[op](self, stmt)
      if result:
        yield result
