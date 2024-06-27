import base64
import copy
import random
import time
import requests

API_ROOT = 'https://%(subdomain)s.confluent.cloud/sql/%(api_version)s'
STMTS_PATH = '/organizations/%(org)s/environments/%(env)s/statements'


def random_id(n=16):
    # Generate a random hex string of length n
    return ('%0' + str(n) + 'x') % random.randrange(16 ** n)


class StatementsEndpointError(Exception):
    pass


class StatementsEndpoint(object):

    def __init__(self, config):
        self.config = config
        self.poll_ms = 300
        self.name_prefix = self.config['flink']['name_prefix']
        self.root_url = self.generate_url()
        d = {}
        for p in ('sql.current-catalog', 'sql.current-database'):
            if p in self.config['flink']:
                d[p] = self.config['flink'][p]
        self.default_properties = d

    @property
    def headers(self):
        credentials = '%s:%s' % (self.config['flink']['api_key'], self.config['flink']['api_secret'])
        return {
            'Content-Type': 'application/json',
            'Authorization': 'Basic %s' % base64.b64encode(credentials.encode()).decode(),
            'User-Agent': 'streamlit_flink_demo'
        }

    def generate_url(self, *path):
        url = (API_ROOT + STMTS_PATH) % {
            'api_version': 'v1',
            'subdomain': self.config['flink']['subdomain'],
            'org': self.config['ccloud']['org'],
            'env': self.config['ccloud']['env'],
        }
        if path:
            path = '/%s' % '/'.join(path)
            url += path
        return url

    def get(self, stmt_name):
        url = self.generate_url(stmt_name)
        r = requests.get(url, headers=self.headers)
        # Don't blow up the caller on 404, just let them deal with it however they want
        if r.status_code == requests.codes.not_found:
            return None
        if r.status_code != requests.codes.ok:
            raise StatementsEndpointError(r)

        return r.json()

    def create(self, sql, properties=None, prefix=None):
        # Any properties passed to this method take precendence over configuration file
        props = copy.deepcopy(self.default_properties)
        props.update(properties or {})
        prefix = prefix or self.name_prefix
        stmt = {
            'name': prefix + random_id(n=12),
            'spec': {
                'compute_pool_id': self.config['flink']['compute_pool'],
                'principal': self.config['flink']['principal'],
                'statement': sql,
                'properties': props
            }
        }
        event = {
            'request': stmt
        }
        headers = self.headers
        print(f'submitting create statement request: {event}, with headers: {headers}')
        r = requests.post(self.root_url, headers=headers, json=stmt)
        if r.status_code != requests.codes.ok:
            raise StatementsEndpointError(r.reason)

        stmt = r.json()
        event = {
            'response': stmt
        }
        stmt_name = stmt['name']
        print(f'created statement: {stmt_name}, event:{event}')
        return stmt

    def next_results(self, url):
        r = requests.get(url, headers=self.headers, allow_redirects=False)
        if r.status_code != requests.codes.ok:
            raise StatementsEndpointError(r)
        r = r.json()
        page = r['results']['data']
        next_url = r['metadata']['next']
        return page, next_url

    def results(self, stmt_name, continuous_query=False):
        url = self.generate_url(stmt_name, 'results')
        row_count = 0
        while True:
            # If we didn't get data or a next url, server is done sending us rows
            if not url:
                if continuous_query:
                    yield None
                else:
                    break

            # We may get a redirect to another domain here, so we can't follow redirects
            # automatically. Like most http libraries, requests will strip the auth header
            # before following a redirect to another domain in order to avoid inadvertently
            # sending it to a domain that the original request didn't ask for.
            # print(f'requesting results with url {url}')
            r = requests.get(url, headers=self.headers, allow_redirects=False)
            if r.status_code == requests.codes.temporary_redirect:
                url = r.next.url
                continue

            if r.status_code != requests.codes.ok:
                raise StatementsEndpointError(r)

            r = r.json()
            page = r['results']['data']
            url = r['metadata']['next']

            if not page:
                # TODO: what is a more reliable way to know we're done?
                # What if the statement just never returned any results?
                # If the last fetch had data but this one did not, we're done
                if continuous_query:
                    # time.sleep(self.poll_ms / 1000.)
                    print(f'[statement name {stmt_name}] no data in page. continuous_query so yielding none')
                    yield None
                elif row_count:
                    print(f'[statement name {stmt_name}] no data in page. not continuous query, but got rows before so breaking out')
                    break
                # TODO: use exponential backoff up to 1s when no results
                # print(f'[statement name {stmt_name}] sleeping for 0.3s before trying to fetch next page of results')
                #time.sleep(self.poll_ms / 1000.)
                yield None
                # Note that since we got data, we immediately hit the results url again for more
            else:
                row_count += len(page)
                for data in page:
                    # Each element of data can take one of two forms (although a given statement will
                    # only return one of these formats, they won't be mixed):
                    #
                    # 1. No changelog: Each element of data looks like this:
                    # {
                    #   'row': ['value1', 'value2']
                    # }
                    #
                    # The row itself is just an array of values, one per column.
                    #
                    # 2. Changelog: Each element of data looks like this:
                    # {
                    #   'op': 0,
                    #   'row': ['value1', 'value2']
                    # }
                    #
                    # Operation is given as an integer in [0, 3]
                    #
                    # The meaning of each operation code is as follows:
                    #
                    # 0 (+I): INSERT, insert a new row into result
                    # 1 (-U): UPDATE_BEFORE, update operation, contains row before update
                    # 2 (+U): UPDATE_AFTER, update operation, contains row after update
                    # 3 (-D): DELETE, delete row from result
                    yield data

    def wait_for_status(self, stmt, *status, timeout=2 * 60):
        if not status:
            raise ValueError('expected at least one status to wait for')
        # Ignore case for status in case the API changes
        status = tuple(map(lambda s: s.lower(), status))
        start = time.time()
        name = stmt['name']
        while time.time() - start < timeout:
            s = self.get(name)
            # Update the caller's statement with the latest metadata
            stmt.update(s)
            phase = s['status']['phase'].lower()
            if phase in status:
                return s
            # If caller isn't waiting for failed status, return None to indicate
            # that the wait operation failed. We do this instead of throw an exception
            # because it gives the caller more freedom around error handling.
            if phase == 'failed':
                return
            time.sleep(self.poll_ms / 1000.)
        # If we never returned, it's a timeout :(
        raise TimeoutError('timed out waiting for status %s for statement %s' % (status, name))
