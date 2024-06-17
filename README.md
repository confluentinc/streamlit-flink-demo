# Demo Streamlit with Kafka and Flink  


# How to run the demo

1. Copy `config.template.ini` to `config.ini` and fill in all values.
2. Install JR via `brew install jr`
3. Copy `kafka-config.template.properties`and `schema-registry-config.template.properties` into `kafka-config.properties` and `schema-registry-config.template.properties` respectively and fill in the values.
4. Run JR to generate some data to the `user`topic. Change the duration if needed.
    ```shell
    jr run user -n 10 -f 0.5s -d 100s -o kafka -s --serializer avro-generic -t user
    ```
5. Start the Streamlit dashboard with `streamlit run dashboard_with_flink.py`.
