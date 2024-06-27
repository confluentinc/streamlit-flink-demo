# Real-lime streaming dashboard with Confluent Cloud, Flink and Streamlit

![streamlit_confluent_cloud_flink_small](https://github.com/confluentinc/streamlit-flink-demo/assets/56603/12c7a18c-02ad-4d1c-9c7e-84fb0b8dc0b5)

# Requirements
For running this demo, you'll need:
- [A Confluent Cloud account](https://www.confluent.io)
- the [JR](https://github.com/ugol/jr) CLI tool to generate data in real-time

# How to run the demo

1. Copy `config.template.ini` to `config.ini` and fill in all values.
2. Copy `kafka-config.template.properties`and `schema-registry-config.template.properties` into `kafka-config.properties` and `schema-registry-config.template.properties` respectively and fill in the values.
3. Install JR via `brew install jr`
4. Run JR to generate some data to the `user`topic. Change the duration if needed.
    ```shell
    jr run user -n 10 -f 0.5s -d 100s -o kafka -s --serializer avro-generic -t user
    ```
6. Start the Streamlit dashboard with `streamlit run dashboard.py`.
