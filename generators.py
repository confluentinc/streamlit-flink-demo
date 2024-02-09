def generate_data_with_flink(n):
    rows = random_array_of_tuples(n)
    values = ', '.join(map(str, rows))
    sql = "INSERT INTO table2 VALUES {}".format(values)

    conf = Config('./config.yml')

    auth = AuthEndpoint(conf)
    print(f'auth={auth}')

    statements = StatementsEndpoint(auth, conf)
    stmt = statements.create(sql)
    print(f'stmt={stmt}')

    ready = statements.wait_for_status(stmt, 'running', 'completed')
    print(f'ready={ready}')

    schema = ready['status']['result_schema']
    print(f'schema={schema}')

    results = statements.results(ready['name'])
    print(f'results={results}')
