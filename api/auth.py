from .errors import EndpointError
import requests
import time


AUTH_URL = 'https://%(domain)s/api/sessions'
DPAT_URL = 'https://%(domain)s/api/access_tokens'
# Refresh auth token every ~10 minutes
AUTH_TIMEOUT = 60 * 10


# Decorator used by endpoint classes to ensure client has a valid DPAT
# token before making any API calls. Keeps auth logic completely independent
# of all API calls themselves.
def require_dpat(fn):
  def inject(self, *args, **kw):
    if self.auth.dpat is None or self.auth.dpat.is_expired:
      t = self.auth.get_auth_token()
      self.auth.dpat = self.auth.get_dpat_token(t)
    return fn(self, *args, **kw)
  return inject


# Simple wrapper around DPAT tokens so that we can keep track of how old a token is
class DataPlaneAccessToken(object):
  def __init__(self, dpat):
    self.token = dpat
    self.obtained = time.time()

  @property
  def age(self):
    # Returns this token's age in fractional seconds
    return time.time() - self.obtained

  @property
  def is_expired(self):
    return self.age > AUTH_TIMEOUT


class AuthenticationError(EndpointError):
  pass


class AuthorizationError(EndpointError):
  pass


class AuthEndpoint(object):

  def __init__(self, conf):
    self.conf = conf
    self.auth_url = AUTH_URL % dict(self.conf['ccloud'].items())
    self.dpat_url = DPAT_URL % dict(self.conf['ccloud'].items())
    self.dpat = None
  
  def get_auth_token(self):
    h = {
      'Content-Type': 'application/json'
    }
    body = {
      'email': self.conf['ccloud']['email'],
      'password': self.conf['ccloud']['password']
    }
    # First get our auth token, we'll use this to get our DPAT later
    r = requests.post(self.auth_url, headers=h, json=body)
    if r.status_code != requests.codes.ok:
      raise AuthenticationError(r)

    return r.json()['token']

  def get_dpat_token(self, auth_token):
    h = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer %s' % auth_token
    }
    # Note that we must pass an empty JSON object as our body here,
    # otherwise the request will be considered malformed and will fail
    r = requests.post(self.dpat_url, headers=h, json={})
    if r.status_code != requests.codes.ok:
      raise AuthorizationError(r)
    
    return DataPlaneAccessToken(r.json()['token'])