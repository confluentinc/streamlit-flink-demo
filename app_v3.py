import asyncio
import random
import altair as alt
import streamlit as st
from pandas import DataFrame

from api.auth import AuthEndpoint
from api.statements import StatementsEndpoint
from lib.config import Config
from lib.flink import Changelog


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


def populate_table(widget, sql, continuous_query):
    conf = Config('./config.yml')
    results, schema = query(conf, sql, continuous_query)
    changelog = Changelog(schema, results)
    changelog.consume(1)
    table = changelog.collapse()
    while True:
        new_data = changelog.consume(1)
        table.update(new_data)
        # wait until we get the update-after to render, otherwise graphs
        # and tables content "jump" around.
        if new_data[0][0] != "-U":
            df = DataFrame(table, None, table.columns)
            df.sort_values(by=df.columns[0], inplace=True)
            widget.dataframe(df, hide_index=True, column_config={"eye_color_count": "frequency", "eyeColor": "Eye color"})
            yield


def populate_chart(widget, sql, continuous_query):
    conf = Config('./config.yml')
    results, schema = query(conf, sql, continuous_query)
    changelog = Changelog(schema, results)
    changelog.consume(1)
    table = changelog.collapse()
    while True:
        new_data = changelog.consume(1)
        table.update(new_data)
        # wait until we get the update-after to render, otherwise graphs
        # and tables content "jump" around.
        if new_data[0][0] != "-U":
            df = DataFrame(table, None, table.columns)
            df = df.astype({'avg_balance': float})

            chart = alt.Chart(df).mark_bar().encode(
                x='age_group',
                y='avg_balance',
                color=alt.Color('age_group', scale=alt.Scale(scheme='category20'))
            ).properties(
                title='Average Balance by Age Group'
            )
            widget.altair_chart(chart, use_container_width=True)

            # widget.bar_chart(df, x="age_group", y="avg_balance", use_container_width=True)
            yield


# noinspection SqlNoDataSourceInspection
async def main():
    st.title("Streamlit ❤️ Confluent Cloud for Flink")

    st.header("Tables")
    eyecolor_frequencies_table = st.empty()

    st.header("Graphs")
    average_balance_table = st.empty()

    await asyncio.gather(
        populate_table(eyecolor_frequencies_table, """
SELECT eyeColor, count(*) AS eye_color_count FROM `user` group by eyeColor
            """, continuous_query=True),

        populate_chart(average_balance_table, """
WITH users_with_age_groups AS
     (SELECT CAST(substring(balance FROM 2) AS DOUBLE) AS balance_double,
             CASE
                 WHEN age BETWEEN 40 AND 49 THEN '40s'
                 WHEN age BETWEEN 30 AND 39 THEN '30s'
                 WHEN age BETWEEN 20 AND 29 THEN '20s'
                 WHEN age BETWEEN 50 AND 59 THEN '50s'
                 ELSE 'other' END AS age_group
      FROM `user`)
SELECT age_group,
       AVG(balance_double) AS avg_balance
FROM `users_with_age_groups`
GROUP BY age_group
    """, continuous_query=True)
    )


asyncio.run(main())
