# Real-time streaming dashboard with Confluent Cloud, Flink and Streamlit

🚀Let's build a real-time streaming dashboard using Confluent Cloud, Flink and Streamlit.

![streamlit_confluent_cloud_flink_small](https://github.com/confluentinc/streamlit-flink-demo/assets/56603/12c7a18c-02ad-4d1c-9c7e-84fb0b8dc0b5)

[Streamlit](https://streamlit.io) is a Python library for building interactive web applications easily with Python scripts. It's ideal for creating dashboards that show real-time data.

In this demo, we'll make a dashboard that shows results from a Flink job processing data in real-time.

You don't need to know Kafka or Flink or install them to run this demo. If you work with data regularly, you likely know enough SQL to understand the Flink queries. We'll guide you through setting everything up.

# Requirements

To run this demo, you'll need:
- A [Confluent Cloud](https://www.confluent.io) account
- The [JR](https://github.com/ugol/jr) CLI tool to generate data in real-time
- The [Streamlit](https://streamlit.io) Python library to build the dashboard

# How to run the demo

To run the demo, just clone this repository and follow these steps:

### Setup a Kafka Cluster and Flink Compute Pool in Confluent Cloud

- No need to install Apache Kafka or Apache Flink on your computer, just follow the instructions on [Getting Started](https://docs.confluent.io/cloud/current/get-started/index.html).

### Install JR and generate data to the Kafka topic

- Create a JR config file for the Kafka and Schema Registry from the template:
    ```shell
    cp kafka-config.template.properties kafka-config.properties && cp schema-registry-config.template.properties schema-registry-config.properties
    ```

- Fill in the values in both files. You can find the values in the Confluent Cloud UI.

- Install JR:

    ```shell
    brew install jr
    ```

- Generate some data to the `user` topic (JR will create the topic automatically if it doesn't exist):

    ```shell
    jr run user -n 10 -f 0.5s -d 100s -o kafka -s --serializer avro-generic -t user
    ```

### Install Python dependencies

- Create a Python virtual environment:

    ```shell
    python -m venv .venv && source .venv/bin/activate
    ```

- Install the required Python packages:

    ```shell
    .venv/bin/pip install -r requirements.txt
    ```

### Run Streamlit

- Create a config file for the Streamlit app from the template:

    ```shell
    cp config.template.ini config.ini
    ```

- Fill in all the values in `config.ini` with the values from your Confluent Cloud setup

- Launch the dashboard with `streamlit run dashboard.py`, a browser window will open automatically.

The dashboard has 3 widgets that are updated in real time:

1. A map showing the user locations around San Francisco:

    ```sql
    SELECT `user`.guid,
           37.7 + (RAND() * (37.77 - 37.7)) AS latitude,
           -122.50 + (RAND() * (-122.39 - (- 122.50))) AS longitude
    FROM `user`
    ```

2. A table showing the frequencies of eye colors:

   ```sql
   SELECT eyeColor, count(*) AS eye_color_count
   FROM `user`
   GROUP BY eyeColor
   ```

3. A chart showing the average bank balance per age group:

   ```sql
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
    ```

If you want to create additional queries, just head over to the Confluent Cloud UI and play with our beautifully designed SQL workspaces.

