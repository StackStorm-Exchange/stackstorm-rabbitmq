# RabbitMQ Integration Pack

Pack which allows integration with [RabbitMQ](http://www.rabbitmq.com/).

## Configuration
### rabbitmq.RabbitMQQueueSensor Sensor
Configuration is required to use the `pika` based RabbitMQ sensor. Copy the example configuration
in [rabbitmq.yaml.example](./rabbitmq.yaml.example) to `/opt/stackstorm/configs/rabbitmq.yaml`
and edit as required.

* ``host`` - RabbitMQ host to connect to.
* ``username`` - Username to connect to RabbitMQ (optional).
* ``password`` - Password to connect to RabbitMQ (optional).
* ``queues`` - List of queues to check for messages. See an example below.
* ``quorum_queues`` - List of queues defined in `queues` that should be handled as `type: quorum`
* ``deserialization_method`` - Which method to use to de-serialize the
  message body. By default, no deserialization method is specified which means
  the message body is left as it is. Valid values are ``json`` and ``pickle``.

You can also use dynamic values from the datastore. See the
[docs](https://docs.stackstorm.com/reference/pack_configs.html) for more info.

You can specify multiple queues using this syntax:

```yaml
---
sensor_config:
  rabbitmq_queue_sensor:
    queues:
      - queue1
      - queue2
      - ....
    quorum_queues:
      - queue2
```

### rabbitmq.QueueWatcherAMQP Sensor
You must configure this sensor under the top level configuration key `amqp_watcher_sensor_config`. This is to ensure backwards compatibility with other sensors.

The below config will declare a simple 'Classic' queue, with a 'direct' exchange with routing key to route messages to that queue (for publishers).

```yaml
---
amqp_watcher_sensor_config:
  host: "rabbitmq.domain.com"
  port: 5672
  username: "guest"
  password: "guest"
  queues:
    - queue: "temp_queue"
      type: "classic"
  exchanges:
    - exchange: "temp_exchange"
      type: "direct"
      bindings:
        - routing_key: "temp.messages"
          queue: "temp_queue"
```
The sensor then monitors any declared queues for new messages, and dispatches the trigger `rabbitmq.amqp_msg_rx` with the data:
```json
{"queue": "queue_name", "body": "message body"}
```

If the message body is a serialized string of JSON, it will be deserialized and loaded before being dispatched.

#### Queues / Exchanges Config Parameters
The sensor uses passthrough via `**kwargs` for declares and binds on `queues` and `exchanges` list items, so follow the [documentation of `py-yaml`](https://docs.celeryq.dev/projects/amqp/en/latest/reference/amqp.channel.html) for the `exchange_declare()`, `queue_declare()`, and `queue_bind()` methods.

**Note:** The `exchange` param is passed into `queue_bind()` explicitly via the sensor inferring it from config structure, and is not required.

## Actions

* ``list_exchanges`` - List available exchanges.
* ``list_queues`` - List available queues.
* ``publish_message`` - Publish a message to a RabbitMQ service.

Note: ``list_exchanges`` and ``list_queues`` invoke ``rabbitmqadmin`` tool and must run on the
same node where RabbitMQ server is running (they connect to the local instance).

### publish_message example

The following action will publish a message to a remote RabbitMQ server with a stunnel-based tunnel preconfigured:

```shell
$ st2 run rabbitmq.publish_message host=localhost port=5673 virtual_host=sensu exchange=metrics exchange_type=topic username=sensu password=password message="foo.bar.baz 1 1436802746"
```

## Sensors

* ``new_message`` - Sensor that triggers a rabbitmq.new_message with a payload containing the queue and the body

Configured in the pack config under the key `sensor_config`

This sensor uses Python's `threading` for concurrency via `pika`'s included methods.

This sensor should only be used with ``fanout`` and ``topic`` exchanges,  this way it doesn't affect the behavior of the app since messages will still be delivered to other consumers / subscribers.
If it's used with ``direct`` or ``headers`` exchanges, those messages won't be delivered to other consumers so it will affect app behavior and potentially break it.

* `amqp_msg_rx` - Sensor that triggers rabbitmq.amqp_msg_rx with a payload containing the queue and the body

Configured in the pack config under the key `amqp_watcher_sensor_config`

This sensor uses `asyncio` for concurrency and `py-amqp` and to handle the declaration of configured queues and exchanges on the remote RabbitMQ instance, and the subsequent consumption of messages.