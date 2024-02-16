# Demo Streamlit with Kafka and Flink  

Generating data with [JR](https://github.com/ugol/jr)
```shell
jr run user -n 10 -f 0.5s -d 10s -o kafka -s --serializer avro-generic -t user
```

