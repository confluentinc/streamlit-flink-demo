from api.auth import AuthEndpoint
from api.statements import StatementsEndpoint
from lib.config import Config
from lib.flink import FlinkSqlInterpreter, Changelog

conf = Config('./config.yml')
flink = FlinkSqlInterpreter(conf)

sql = """
SELECT * from table2;
"""

# rs = flink.execute(sql)
auth = AuthEndpoint(conf)
statements = StatementsEndpoint(auth, conf)
stmt = statements.create(sql)
ready = statements.wait_for_status(stmt, 'running', 'completed')
table = None

results = statements.results(ready['name'])

# for x in results:
#     print(f'x={x}')

schema = ready['status']['result_schema']
changelog = Changelog(schema, results)

consumed_rows = changelog.consume(10)
print(f', got {len(consumed_rows)} new rows={consumed_rows}')

consumed_rows = changelog.consume(10)
print(f', got {len(consumed_rows)} new rows={consumed_rows}')

consumed_rows = changelog.consume(10)
print(f', got {len(consumed_rows)} new rows={consumed_rows}')

consumed_rows = changelog.consume(10)
print(f', got {len(consumed_rows)} new rows={consumed_rows}')
