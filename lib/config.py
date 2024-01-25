import copy
import yaml


class Config(dict):

  def __init__(self, path='', raw=None):
    super().__init__()
    self.path = path
    if raw:
      self.update(yaml.load(raw, yaml.Loader))
    else:
      if not self.path:
        raise ValueError('no configuration file specified')
      with open(self.path) as f:
        self.update(yaml.load(f, yaml.Loader))
    self._validate()

  def _validate(self):
    reserved = {'ccloud', 'flink'}
    for section in reserved:
      if section not in self:
        raise ValueError("required section '%s' not found in '%s'" % (section, self.path or 'raw'))
    keys = self.keys() - reserved
    for catalog in keys:
      sr = 'schema_registry'
      if sr not in self[catalog]:
        raise ValueError("required key '%s' not found under catalog '%s'" % (sr, catalog))
      for k in ('endpoint', 'key', 'secret'):
        if k not in self[catalog][sr]:
          raise ValueError("required key '%s' not found under '%s'" % (k, sr))
      for db in self[catalog].keys() - {sr}:
        for k in ('endpoint', 'key', 'secret'):
          if k not in self[catalog][db]:
            raise ValueError("required key '%s' not found under database '%s'" % (k, db))

  @property
  def catalogs(self):
    # Returns a dictionary of the form:
    #  {
    #    'catalog': [
    #      'database1',
    #      'database2',
    #    ]
    #    'catalog2': [...]
    #  }
    d = copy.deepcopy(self)
    del d['ccloud']
    del d['flink']

    for dbs in d.values():
      del dbs['schema_registry']

    return d

  def sanitized(self):
    # Mapping of field names to mask, and how many bytes to leave unmasked
    secrets = {
      'password': 0,
      'secret': 4
    }
    def mask(s, keep=0):
      unmasked = s[(len(s) - keep):]
      return '...' + unmasked
    def sanitize(d):
      r = {}
      for k, v in d.items():
        if k in secrets:
          v = mask(v, keep=secrets[k])
        if isinstance(v, dict):
          v = sanitize(v)
        r[k] = v
      return r

    result = {}
    for s in self.keys():
      result[str(s)] = sanitize(self[s])
    return result