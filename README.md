# Real-time streaming dashboard with Kafka, Flink SQL, Streamlit and Confluent Cloud.

🚀Let's build a real-time streaming dashboard using Apache Kafka, Flink SQL and Streamlit.

![streamlit_confluent_cloud_flink_small](https://github.com/confluentinc/streamlit-flink-demo/assets/56603/12c7a18c-02ad-4d1c-9c7e-84fb0b8dc0b5)

[Streamlit](https://streamlit.io) is a Python library for building interactive web applications easily with Python scripts. It's ideal for creating dashboards that show real-time data.

In this demo, we'll build a dashboard that shows results from a Flink job processing data in real-time.

You don't need to know Apache Kafka or Apache Flink or install them to run this demo. If you work with data regularly, you likely know enough SQL to understand the Flink queries. We'll guide you through setting everything up.


# How to run the demo

**Overview**

To run this demo, you will:
- Create a [Confluent Cloud](https://www.confluent.io) account
- Set up an environment with a Kafka cluster and a Flink compute pool in Confluent Cloud
- Generate random data with the [JR](https://github.com/ugol/jr) CLI tool
- Run the Streamlit dashboard on your machine
- Destroy the Confluent Cloud environment to avoid incurring charges.

Let's get started!

### Step 1: Clone this repository

First, you need to clone this repository to your local machine:

```shell
git clone https://github.com/confluentinc/streamlit-flink-demo && cd streamlit-flink-demo
```

### Step 2 : Set up a Kafka Cluster and a Flink Compute Pool in Confluent Cloud

No need to install Kafka or Flink on your computer when you have a Confluent Cloud account:

First, sign up for [Confluent Cloud](https://www.confluent.io/confluent-cloud).

#### To add an environment:

- Open the Confluent Cloud UI and go to the Environments page at https://confluent.cloud/environments.
- Click 'Add cloud environment'.
- Enter a name for the environment: `streamlit-flink-kafka-demo`.
- Click 'Create'.

#### Then, create a cluster inside your environment.

- On the 'Create cluster' page, for the 'Basic' cluster, select 'Begin configuration'.
- When you're prompted to select a provider and location, choose AWS's `Ohio (us-east-2)` and `Single Zone`
- Click 'Continue'.
- Specify a cluster name, eg `streamlit-flink-kafka-cluster` , review the configuration and cost information, and then select 'Launch cluster'.
- Depending on the chosen cloud provider and other settings, it may take a few minutes to provision your cluster,
but after the cluster has provisioned, the 'Cluster Overview' page displays.

#### Create a Kafka Cluster API key and save it.

- From the 'Administration' menu in the top right menu, click 'API keys'
- Click 'Add key'.
- Choose to create the key associated with your user account.
- Select the environment and the cluster you created earlier,
- The API key and secret are generated and displayed.
- Click 'Copy' to copy the key and secret to a secure location, you'll need them later.

#### Create an API key for Schema Registry.

- In the environment for which you want to set up Schema Registry, find 'Credentials' on the right side panel and
click <someNumber> keys to bring up the API credentials dialog. (If you are just getting started, click 0 keys.)
- Click 'Add key' to create a new Schema Registry API key.
- When the API Key and API Secret are saved, click the checkbox next to 'I have saved my API key and secret and
am ready to continue', and click 'Continue'. Your new Schema Registry key is shown on the Schema Registry API access
key list. Save the Schema Registry key and secret to a secure location, you'll need them later.

#### Create a Flink compute pool.

- In the navigation menu, click 'Environments' and click the tile for the environment where you want to use Flink SQL.
- In the environment details page, click 'Flink'.
- In the Flink page, click 'Compute pools', if it’s not selected already.
- Click 'Create compute pool' to open the 'Create compute pool' page.
- In the 'Region' dropdown, select the region that hosts the data you want to process with SQL. Click 'Continue'.

*Important*
_This region must the same region as the one in which you created your cluster, that is, AWS's `us-east-2`_
- In the 'Pool' name textbox, enter “streamlit-flink-kafka-compute-pool”.
- In the 'Max CFUs' dropdown, select 10.
- Click 'Continue', and on the 'Review and create' page, click 'Finish'.

    A tile for your compute pool appears on the Flink page. It shows the pool in the Provisioning state.
_It may take a few minutes for the pool to enter the Running state._


### Step 3: Install JR and generate data to the Kafka topic

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

### Step 4: Install Python dependencies

- Create a Python virtual environment:

    ```shell
    python3 -m venv .venv && source .venv/bin/activate
    ```

- Install the required Python packages:

    ```shell
    .venv/bin/pip install -r requirements.txt
    ```

### Step 5: Run Streamlit

- Create a config file for the Streamlit app from the template:

    ```shell
    cp config.template.ini config.ini
    ```

- Fill in all the values in `config.ini` with the values from your Confluent Cloud setup

- Launch the dashboard with `streamlit run dashboard.py`, a browser window will open automatically.

### Step 6: Explore the dashboard

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

If you want to experiment with different queries, just head over to the Confluent Cloud UI and play with our beautifully designed SQL workspaces.

<img width="1041" alt="Flink workspace" src="https://github.com/confluentinc/streamlit-flink-demo/assets/56603/4192c891-0029-4703-9854-dfceb7820ece">

### Step 7: Clean up

When you're done, you can destroy the Confluent Cloud environment to avoid incurring charges.

# Disclaimer
The code in this repository is not supported by Confluent and is intended solely for demonstration purposes, not for production use.

