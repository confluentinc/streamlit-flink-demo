import subprocess
import asyncio
import streamlit as st
import pandas as pd
from confluent_kafka import KafkaError
from confluent_kafka.avro import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

create_table_statement = 'CREATE TABLE random_int_table_avg(counter_id INT, avg_value INT, PRIMARY KEY (counter_id) NOT ENFORCED);'
populate_table_statement = 'INSERT INTO random_int_table_avg SELECT 1, avg(random_value) from random_int_table GROUP by 1;'

def run_flink_statement(statement):
    st.write("Running statement: " + statement)
    flink_compute_pool = 'lfcp-g9wpk1'
    kafka_cluster = 'lkc-jv153q'
    subprocess.run(
        f"confluent flink statement create --compute-pool {flink_compute_pool} --database {kafka_cluster} --sql '{statement}' --wait", shell=True)


async def read_messages(placeholder):
    run_flink_statement(create_table_statement)
    run_flink_statement(populate_table_statement)

    # Kafka and Schema Registry Configuration
    conf = {
        'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'group.id': 'streamlit-confluent-cloud-flink-4',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'H4IDDTE23NXUHNK4',
        'sasl.password': '0X6kQS9IMIDaHL2bVIjdpyVFRy5ctWg27Kx5W5oJo4FaCaKaxmLkh1Qyg9WcwQj4',
    }

    # Schema Registry configuration
    schema_registry_conf = {
        'url': 'https://psrc-mw0d1.us-east-2.aws.confluent.cloud',
        'basic.auth.user.info': '4KCWWPHCUO5AJRPE:Mj4Lg69xp/7W9He0uS2+UGqqupWz2audvBqJ/sYlBYHkbN6R0f6vxeivJwnBIHaD'
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Avro Deserializer for key and value
    avro_value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)

    consumer = Consumer(conf)

    topic = "random_int_table"
    st.write(f"Subscribing to topic {topic}")
    consumer.subscribe([topic])

    # Collecting data for DataFrame
    data = []
    i = 0
    try:

        while True:
            try:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print("Error encountered: {}".format(msg.error()))

                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        st.write("Reached end of partition")
                        continue
                    else:
                        print(msg.error())
                        break

                # Manually deserialize Avro message
                # st.write("Got a new message")

                value = avro_value_deserializer(msg.value(), None)
                with placeholder:
                    data.append(value)
                    st.write(pd.DataFrame(data))
                await asyncio.sleep(1)

            except SerializerError as e:
                print("Message deserialization failed: {}".format(e))
                break

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

async def main():
    st.header("Reading from a Flink SQL table (backing topic)")
    placeholder = st.empty()
    await read_messages(placeholder)


asyncio.run(main())
