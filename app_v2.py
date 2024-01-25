import asyncio
import streamlit as st
import pandas as pd
from lib.config import Config
from lib.flink import FlinkSqlInterpreter
from lib.kafka import AvroProducer
import random


async def read_messages(placeholder):
    # This config contains CCloud API keys, endpoints, etc.
    conf = Config('./config.yml')
    flink = FlinkSqlInterpreter(conf)

    # Create a table to write rows into
    rs = flink.execute("""
    CREATE TABLE IF NOT EXISTS table1 (x INTEGER, y INTEGER);
    """)

    # execute() always returns an generator of result sets, one for each statement given.
    # We're calling next() here as a way to succinctly consume the first result set since
    # we know we only have one.
    # console.print(next(rs))

    # Generate some events and produce them to a topic
    await generate_events(conf, 'table1', 10)

    # Run a SQL statement over these events and render the results to the
    # console, then clean up the table when we're done
    data = []
    rs = flink.execute("""
    SELECT x, AVG(y) FROM table1 GROUP BY x;
    """)

    data = [row for changelog in rs for row in changelog.rows]
    st.write(pd.DataFrame(data))

    # Deletes all statements created during this session
    flink.cleanup()


async def generate_events(conf, topic_name, number_of_events):
    events = []
    for n in range(number_of_events):
        event = {
            'topic': topic_name,
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


async def main():
    st.header("Reading from a Flink SQL table (backing topic)")
    placeholder = st.empty()
    await read_messages(placeholder)


asyncio.run(main())
