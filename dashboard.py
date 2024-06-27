from configparser import ConfigParser

import altair as alt
import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
from api.statements import StatementsEndpoint
from lib.flink import Changelog


def main():
    st.set_page_config(layout="wide")

    st.title("Streamlit ❤️ Confluent Cloud for Flink")

    st.header("User Locations", divider="rainbow")

    if 'user_locations' not in st.session_state:
        st.session_state['user_locations'] = {"center": [37.735, -122.43], "zoom": 11, "markers": []}

    if 'eye_colors' not in st.session_state:
        st.session_state.eye_colors = {"dataframe": pd.DataFrame()}

    if 'users_per_age_groups' not in st.session_state:
        columns = ['age_group', 'avg_balance']
        df = pd.DataFrame(columns=columns)
        st.session_state['users_per_age_groups'] = {"dataframe": df}

    first_column, second_column = st.columns([1, 2])

    with first_column:
        if st.button("Clear markers"):
            st.session_state['user_locations']['markers'] = []

        if st.toggle("Refresh the map automatically"):
            st.experimental_fragment(fetch_result_page_user_locations, run_every=0.3)()

    with second_column:
        draw_map()

    st.header("Eye color frequency", divider="rainbow")

    if st.toggle("Refresh the table automatically"):
        st.experimental_fragment(fetch_result_page_eye_colors, run_every=0.5)()

    draw_table()

    st.header("Average Balance by Age Group", divider="rainbow")

    if st.toggle("Refresh the chart automatically"):
        st.experimental_fragment(fetch_result_page_users_per_age_group, run_every=0.3)()

    draw_chart()


def read_config(config_file='config.ini'):
    config = ConfigParser()
    config.read(config_file)
    if not config.sections():
        print(f'Cannot read configuration file: {config_file}')
        return None
    return config


def create_marker(latitude, longitude):
    return folium.Marker(
        location=[latitude, longitude],
        popup=f"Marker at {latitude:.2f}, {longitude:.2f}",
    )


def update_table_with_changelog(query_id):
    changelog = st.session_state[query_id]['changelog']
    print(f'Retrieved changelog with object id: {id(changelog)} for query_id: {query_id}')
    table = st.session_state[query_id]['table']
    new_data = changelog.consume(1)
    table.update(new_data)
    return table, new_data


def fetch_result_page_eye_colors():
    query_id = "eye_colors"
    run_flink_query("SELECT eyeColor, count(*) AS eye_color_count FROM `user` group by eyeColor", query_id)
    changelog = st.session_state[query_id]['changelog']
    print(f'[fetch_result_page_eye_colors] Retrieved changelog with object id: {id(changelog)} for query_id: {query_id}')
    table = st.session_state[query_id]['table']
    new_data = changelog.consume(1)
    table.update(new_data)
    st.session_state[query_id]['table'] = table
    st.session_state[query_id]['changelog'] = changelog
    # table, new_data = update_table_with_changelog(query_id)

    # Dont' update on UPDATE_BEFORE to avoid table 'jumps'.
    if new_data and new_data[0] and new_data[0][0] != "-U":
        df = pd.DataFrame(table, None, table.columns)
        df.sort_values(by=df.columns[0], inplace=True)
        st.session_state[query_id]['dataframe'] = df


def fetch_result_page_user_locations():
    query_id = "user_locations"
    run_flink_query(
        "SELECT `user`.guid, 37.7 + (RAND() * (37.77 - 37.7)) AS latitude, -122.50 + (RAND() * (-122.39 - (- 122.50))) AS longitude FROM `user`",
        query_id)
    # table, new_data = update_table_with_changelog(query_id)
    changelog = st.session_state[query_id]['changelog']
    print(f'[fetch_result_page_user_locations] Retrieved changelog with object id: {id(changelog)} for query_id: {query_id}')
    table = st.session_state[query_id]['table']
    new_data = changelog.consume(1)
    table.update(new_data)
    st.session_state[query_id]['table'] = table
    st.session_state[query_id]['changelog'] = changelog
    # Here we keep adding markers, but we could do something different based on the op.
    for item in new_data:
        op, guid, latitude, longitude = item
        if op != "-U":
            marker = create_marker(float(latitude), float(longitude))
            st.session_state[query_id]["markers"].append(marker)


def fetch_result_page_users_per_age_group():
    query_id = "users_per_age_groups"
    run_flink_query("""
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
                    """,
                    query_id)
    # table, new_data = update_table_with_changelog(query_id)
    changelog = st.session_state[query_id]['changelog']
    print(f'[fetch_result_page_users_per_age_group] Retrieved changelog with object id: {id(changelog)} for query_id: {query_id}')
    table = st.session_state[query_id]['table']
    new_data = changelog.consume(1)
    table.update(new_data)
    st.session_state[query_id]['table'] = table
    st.session_state[query_id]['changelog'] = changelog

    if new_data and new_data[0] and new_data[0][0] != "-U":
        df = pd.DataFrame(table, None, table.columns)
        df = df.astype({'avg_balance': float})
        st.session_state[query_id]['dataframe'] = df


@st.experimental_fragment(run_every=1)
def draw_chart():
    df = st.session_state['users_per_age_groups']['dataframe']
    chart = alt.Chart(df).mark_bar().encode(
        x='age_group',
        y='avg_balance',
        color=alt.Color('age_group', scale=alt.Scale(scheme='category20'))
    ).properties(
        title='Average Balance by Age Group'
    )
    st.altair_chart(chart, use_container_width=True)


@st.cache_data
def base_map():
    m = folium.Map(
        location=st.session_state['user_locations']["center"],
        zoom_start=st.session_state['user_locations']["zoom"]
    )
    fg = folium.FeatureGroup(name="Markers")
    return m, fg


@st.experimental_fragment(run_every=1)
def draw_map():
    m, fg = base_map()

    for marker in st.session_state['user_locations']["markers"]:
        fg.add_child(marker)

    st_folium(
        m,
        feature_group_to_add=fg,
        center=st.session_state['user_locations']["center"],
        zoom=st.session_state['user_locations']["zoom"],
        key="user_locations_map",
        returned_objects=[],
        use_container_width=True,
        height=400,
    )


@st.experimental_fragment(run_every=1)
def draw_table():
    df = st.session_state['eye_colors']['dataframe']
    st.dataframe(df, hide_index=True, column_config={'eye_color_count': 'frequency', 'eyeColor': 'Eye color'})


@st.cache_resource
def run_flink_query(sql, query_id):
    config = read_config()
    statements = StatementsEndpoint(config)
    stmt = statements.create(sql)
    ready = statements.wait_for_status(stmt, 'running', 'completed')
    schema = ready['status']['traits']['schema']
    statement_name = ready['name']
    # this is an async generator, not a blocking function
    results = statements.results(statement_name, True)
    changelog = Changelog(schema, results)
    changelog.consume(1)
    table = changelog.collapse()
    st.session_state[query_id]['table'] = table
    st.session_state[query_id]['changelog'] = changelog


if __name__ == "__main__":
    main()
