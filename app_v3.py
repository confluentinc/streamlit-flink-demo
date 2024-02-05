import asyncio
import random
import time

import streamlit as st
from pandas import DataFrame

from api.auth import AuthEndpoint
from api.statements import StatementsEndpoint
from lib.config import Config
from lib.flink import Changelog
from lib.kafka import AvroProducer


def populate_table(widget, sql, continuous_query):
    conf = Config('./config.yml')
    print(f'conf={conf}')
    results, schema = query(conf, sql, continuous_query)
    changelog = Changelog(schema, results)
    changelog.consume(1)
    table = changelog.collapse()
    while True:
        new_data = changelog.consume(1)
        table.update(new_data)
        widget.write(DataFrame(table, None, table.columns))
        yield


def query(conf, sql, continuous_query):
    auth = AuthEndpoint(conf)
    statements = StatementsEndpoint(auth, conf)
    stmt = statements.create(sql)
    ready = statements.wait_for_status(stmt, 'running', 'completed')
    schema = ready['status']['result_schema']
    results = statements.results(ready['name'], continuous_query)
    return results, schema


def random_array_of_tuples(n):
    return [(random.randint(1, 10), random.randint(1, 1000)) for _ in range(n)]


def generate_data_with_flink(n):
    rows = random_array_of_tuples(n)
    values = ', '.join(map(str, rows))
    sql = "INSERT INTO table2 VALUES {}".format(values)

    conf = Config('./config.yml')

    auth = AuthEndpoint(conf)
    print(f'auth={auth}')

    statements = StatementsEndpoint(auth, conf)
    stmt = statements.create(sql)
    print(f'stmt={stmt}')

    ready = statements.wait_for_status(stmt, 'running', 'completed')
    print(f'ready={ready}')

    schema = ready['status']['result_schema']
    print(f'schema={schema}')

    results = statements.results(ready['name'])
    print(f'results={results}')


def generate_data_with_kafka(number_of_events):
    conf = Config('./config.yml')
    events = []
    for n in range(number_of_events):
        event = {
            'topic': 'table2',
            'row': {
                'x': random.randint(1, 10),
                'y': random.randint(1, 1000)
            }
        }
        events.append(event)
    print("Producing events...")
    p = AvroProducer(conf)
    p.produce(events)
    print(f'Produced {len(events)} events.')


def start_generating_data_with_flink(n: int, every: int):
    while True:
        generate_data_with_flink(n)
        time.sleep(every)


def start_generating_data_with_kafka(n: int, every: int):
    while True:
        generate_data_with_kafka(n)
        time.sleep(every)


# noinspection SqlNoDataSourceInspection
async def main():
    st.header("Reading from a Flink SQL table (backing topic)")

    average_table = st.empty()

    row_count_table = st.empty()

    # await populate_table(average_table, """
    # SELECT x,
    #        AVG(y) as average,
    #        MIN(y) as min_y,
    #        MAX(y) as max_y
    # FROM table2 GROUP BY x;
    # """)

    # await populate_table(row_count_table, """
    # SELECT x, count(y) as count_y
    # FROM table2 GROUP BY x;
    # """)

    await asyncio.gather(

        populate_table(row_count_table, """
    SELECT x, count(y) as count_y
    FROM table2 GROUP BY x;
    """, continuous_query=True),

        populate_table(average_table, """
    SELECT x,
           AVG(y) as average,
           MIN(y) as min_y,
           MAX(y) as max_y
    FROM table2 GROUP BY x;
    """, continuous_query=True)
    )


asyncio.run(main())
