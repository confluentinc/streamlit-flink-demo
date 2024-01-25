import json


def format_response(error, response, recurse_history=True):
  headers = []
  for k, v in response.headers.items():
    headers.append(' %s: %s' % (k, v))
  headers = '\n'.join(headers)

  # If this response has redirect history, recursively format
  # each response underneath the top-level error response. Note
  # that we only recurse into one level of history.
  history = []
  if recurse_history:
    for h in response.history:
      fh = format_response(None, h, recurse_history=False)
      history.append(fh)
    history = '\n'.join(history)

  e = []
  if isinstance(error, (dict, list)):
    error = json.dumps(error, indent=1)
  error = error or ''
  if error:
    e.append([error.strip(), '\n'])
  e.append([response.url])
  e.append(['Method:', ' ', response.request.method])
  e.append(['Status:', ' ', str(response.status_code)])
  body = response.request.body or ''
  if body:
    # Pretty print the request payload
    body = json.dumps(json.loads(body), indent=1)

  e.append(['Body:', ' ', body])
  e.append(['Headers:', '\n', headers])
  if history:
    e.append(['History:', '\n', history])

  f = [''.join(row) for row in e]
  s = '\n'.join(f)

  return s


class EndpointError(Exception):

  def __init__(self, response):
    self.response = response
    # If we can't parse the response as JSON, fall back to raw text
    try:
      # Try to interpret response as JSON
      self.error = response.json()
    except json.decoder.JSONDecodeError:
      self.error = response.text

  def __str__(self):
    err = self.__class__.__name__ + ': '
    err += format_response(self.error, self.response)
    return err  