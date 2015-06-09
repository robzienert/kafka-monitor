# kafka-monitor

Kafka monitoring plugin for influxdb.

`$ java -jar kafka-monitor-all.jar config.groovy`

## config.groovy

    zkconnect = '127.0.0.1:2181'
    topic = 'test'
    group = 'test'

    influxdb {
        connect = 'http://127.0.0.1:8086'
        dbName = 'test'
        username = 'guest'
        password = 'guest'
    }
