import asyncio
import base64
import aiohttp
import copy
import random
import time

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

    async def get(self, stmt_name):
        url = self.generate_url(stmt_name)

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as r:
                if r.status == 404:
                    return None
                if r.status != 200:
                    raise StatementsEndpointError(r)
                json = await r.json()
                return json

    async def create(self, sql, properties=None, prefix=None):
        start_time = time.time()
        # Any properties passed to this method take precedence over configuration file
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
        async with aiohttp.ClientSession() as session:
            async with session.post(self.root_url, headers=self.headers, json=stmt) as r:
                if r.status != 200:
                    raise StatementsEndpointError(r)
                stmt_res = await r.json()
                print('created statement %s' % stmt_res['name'])
                return stmt_res

    async def results(self, stmt_name, continuous_query=False):
        url = self.generate_url(stmt_name, 'results')
        row_count = 0
        # If we didn't get data or a next url, server is done sending us rows
        while url:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, allow_redirects=False) as resp:
                    if resp.status != 200:
                        raise StatementsEndpointError(resp)

                    resp = await resp.json()

                    page = resp['results']['data']
                    url = resp['metadata']['next']

                    if not page:
                        if not continuous_query and row_count:
                            # If the last fetch had data but this one did not, we're done polling results
                            break
                        # No data yet, so wait a little bit before trying again
                        # TODO: Implement exponential backoff here
                        await asyncio.sleep(self.poll_ms / 1000.)
                        continue

                    # Note that since we got data, we immediately hit the results url again for more
                    row_count += len(page)
                    for data in page:
                        # Operation is given as an integer in [0, 3]
                        #
                        # The meaning of each operation code is as follows:
                        #
                        # 0 (+I): INSERT, insert a new row into result
                        # 1 (-U): UPDATE_BEFORE, update operation, contains row before update
                        # 2 (+U): UPDATE_AFTER, update operation, contains row after update
                        # 3 (-D): DELETE, delete row from result
                        yield data
                    await asyncio.sleep(0.001)

    async def wait_for_status(self, stmt, *status, timeout=2 * 60):
        if not status:
            raise ValueError('expected at least one status to wait for')
        # Ignore case for status in case the API changes
        status = tuple(s.lower() for s in status)
        start_time = time.time()
        name = stmt['name']
        while time.time() - start_time < timeout:
            s = await self.get(name)
            # Update the caller's statement with the latest metadata
            stmt.update(s)
            phase = s['status']['phase'].lower()
            if phase in status:
                return s
            # If caller isn't waiting for failed status, return None to indicate
            # that the wait operation failed. We do this instead of throw an exception
            # because it gives the caller more freedom around error handling.
            if phase == 'failed':
                return None
            await asyncio.sleep(self.poll_ms / 1000.)
        # If we never returned, it's a timeout :(
        raise TimeoutError(f'Timed out waiting for status {status} for statement {name}')
