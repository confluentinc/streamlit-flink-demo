import asyncio
import random
import altair as alt
import streamlit as st
from pandas import DataFrame

from api.auth import AuthEndpoint
from api.statements import StatementsEndpoint
from lib.config import Config
from lib.flink import Changelog


async def query(conf, sql, continuous_query):
    auth = AuthEndpoint(conf)
    statements = StatementsEndpoint(auth, conf)

    stmt = await statements.create(sql)
    ready = await statements.wait_for_status(stmt, 'running', 'completed')
    schema = ready['status']['result_schema']
    name = ready['name']
    # this is an async generator, not a blocking function
    results = statements.results(name, continuous_query)
    return results, schema


def random_array_of_tuples(n):
    return [(random.randint(1, 10), random.randint(1, 1000)) for _ in range(n)]


async def populate_table(widget, sql, continuous_query):
    conf = Config('./config.yml')
    results, schema = await query(conf, sql, continuous_query)
    changelog = Changelog(schema, results)
    await changelog.consume(1)
    table = changelog.collapse()
    while True:
        new_data = await changelog.consume(1)
        table.update(new_data)
        # wait until we get the update-after to render, otherwise graphs
        # and tables content "jump" around.
        if new_data[0][0] != "-U":
            df = DataFrame(table, None, table.columns)
            df.sort_values(by=df.columns[0], inplace=True)
            widget.dataframe(df, hide_index=True,
                             column_config={"eye_color_count": "frequency", "eyeColor": "Eye color"})
        await asyncio.sleep(0.01)


async def populate_map(widget, sql, continuous_query):
    conf = Config('./config.yml')
    results, schema = await query(conf, sql, continuous_query)
    changelog = Changelog(schema, results)
    await changelog.consume(1)
    table = changelog.collapse()
    while True:
        new_data = await changelog.consume(10)
        table.update(new_data)
        # wait until we get the update-after to render, otherwise graphs
        # and tables content "jump" around.
        if new_data[0][0] != "-U":
            df = DataFrame(table, None, table.columns)
            df = df.astype({'latitude': float, 'longitude': float})
            widget.map(df, use_container_width=True)
        await asyncio.sleep(0.25)


async def populate_chart(widget, sql, continuous_query):
    conf = Config('./config.yml')
    results, schema = await query(conf, sql, continuous_query)
    changelog = Changelog(schema, results)
    await changelog.consume(1)
    table = changelog.collapse()
    while True:
        new_data = await changelog.consume(1)
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

        await asyncio.sleep(0.01)


# noinspection SqlNoDataSourceInspection
async def main():
    st.title("Streamlit ❤️ Confluent Cloud for Flink")

    col1, col2 = st.columns(2)

    with col1:
        st.header("Tables")
        eyecolor_frequencies_table = st.empty()

        st.header("Graphs")
        average_balance_table = st.empty()

    with col2:
        st.header("Maps")
        map_of_users = st.empty()

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
        """, continuous_query=True),

        populate_map(map_of_users, """
    SELECT 37.704 + (RAND() * (37.812 - 37.704)) AS latitude,
    -122.527 + (RAND() * (-122.357 + 122.527)) AS longitude FROM `user`
        """, continuous_query=True)
    )


# if __name__ == "__main__":
asyncio.run(main())
