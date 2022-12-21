import copy
import functools
import json
import pickle
import time

import pika
import pika.exchange_type

from st2reactor.sensor.base import Sensor


class RabbitMQBoundQueueSensor(Sensor):
    TRIGGER = "rabbitmq.routed_message"

    def __init__(self, sensor_service, config=None) -> None:
        super(RabbitMQBoundQueueSensor, self).__init__(
            sensor_service=sensor_service, config=config
        )
        self._logger = self.sensor_service.get_logger(name=self.__class__.__name__)
        self._config = config
        self.sensor_config = self._config["sensor_binding_config"]
        self._message_dispatch = functools.partial(
            self._sensor_service.dispatch, trigger=self.TRIGGER
        )
        self._consumer = ReconnectingConsumer(
            self._logger, self.sensor_config, self._message_dispatch
        )
        self._reconnect_delay = 0

    def run(self):
        while True:
            self._consumer.run()
            self._maybe_reconnect()

    def cleanup(self):
        self._consumer.stop()

    def setup(self):
        self._consumer.connect()

    def add_trigger(self, trigger):
        pass

    def update_trigger(self, trigger):
        pass

    def remove_trigger(self, trigger):
        pass

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            self._logger.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = ReconnectingConsumer(
                self._logger, self.sensor_config, self._message_dispatch
            )
            self._consumer.connect()

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


class ReconnectingConsumer:
    AMQP_PREFETCH = 1
    DEFAULT_USERNAME = pika.ConnectionParameters.DEFAULT_USERNAME
    DEFAULT_PASSWORD = pika.ConnectionParameters.DEFAULT_PASSWORD
    DEFAULT_EXCHANGE = pika.exchange_type.ExchangeType.direct
    DESERIALIZATION_FUNCTIONS = {"json": json.loads, "pickle": pickle.loads}

    def __init__(self, logger, config, message_dispatch) -> None:
        self.should_reconnect = False
        self.was_consuming = False

        self._logger = logger
        self._config = config
        self._dispatch = message_dispatch
        self._host = self._config["host"]
        self._username = self._config.get("username", self.DEFAULT_USERNAME)
        self._password = self._config.get("password", self.DEFAULT_PASSWORD)
        self._conn = None
        self._channel = None
        self._closing = False
        self._consuming = False
        self._consumer_tags = set()

        self._deserialization_method = self._config.get(
            "deserialization_method", "json"
        )
        if self._deserialization_method not in self.DESERIALIZATION_FUNCTIONS:
            raise ValueError(
                "Invalid deserialization method specified: %s"
                % (self._deserialization_method)
            )

    def run(self):
        self._conn.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            self._logger.info("Stopping")
            if self._consuming:
                self._stop_consuming()
                self._conn.ioloop.start()
            else:
                self._conn.ioloop.stop()
            self._logger.info("Stopped")

    def connect(self):
        credentials = pika.PlainCredentials(self._username, self._password)
        connection_params = pika.ConnectionParameters(
            host=self._host, credentials=credentials
        )
        self._conn = self._open_connection(connection_params)

    def _reconnect(self):
        self.should_reconnect = True
        self.stop()

    def _open_connection(self, params):
        return pika.SelectConnection(
            params,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_close,
        )

    def _close_connection(self):
        self._consuming = False
        if self._conn.is_closing or self._conn.is_closed:
            self._logger.debug("Connection is closing or already closed")
        else:
            self._logger.debug("Closing connection")
            self._conn.close()

    def _on_connection_open(self, connection):
        self._logger.debug("Connection opened")
        self._open_channel()

    def _on_connection_open_error(self, connection, err):
        self._logger.error("Connection open failed: %s", err)
        self._reconnect()

    def _on_connection_close(self, connection, reason):
        self._channel = None
        if self._closing:
            self._conn.ioloop.stop()
        else:
            self._logger.warning("Connection closed, reconnect necessary: %s", reason)
            self._reconnect()

    def _open_channel(self):
        self._logger.debug("Creating new channel")
        self._conn.channel(on_open_callback=self._on_channel_open)

    def _close_channel(self):
        self._logger.debug("Closing channel")
        self._channel.close()

    def _on_channel_open(self, channel):
        self._logger.debug("Channel opened")
        self._channel = channel

        self._logger.debug("Adding channel close callback")
        self._channel.add_on_close_callback(self._on_channel_closed)

        self._logger.debug("Setting channel prefetch: %s", self.AMQP_PREFETCH)
        self._channel.basic_qos(
            prefetch_count=self.AMQP_PREFETCH, callback=self._on_basic_qos_ok
        )

    def _on_channel_closed(self, channel, reason):
        self._logger.warning("Channel %i was closed: %s", channel, reason)
        self._close_connection()

    def _on_basic_qos_ok(self, frame):
        self._logger.debug("QOS set to: %s", self.AMQP_PREFETCH)
        self._setup_exchanges()

    def _setup_exchanges(self):
        self._logger.debug("Setting up configured exchanges")
        for exchange in self._config.get("exchanges", list()):
            name = exchange["name"]
            exchange_type = exchange.get("exchange_type", self.DEFAULT_EXCHANGE)
            queues = exchange.get("queues", list())
            self._logger.debug("Declaring exchange: %s", name)

            # pass config object as extra argument in callback
            callback = functools.partial(
                self._on_exchange_declare_ok, userdata=(queues, name)
            )
            self._channel.exchange_declare(
                exchange=name, exchange_type=exchange_type, callback=callback
            )

    def _on_exchange_declare_ok(self, frame, userdata):
        queues, exchange = userdata
        self._logger.debug("Exchange declared: %s", exchange)
        self._setup_queues(queues, exchange)

    def _setup_queues(self, queues, exchange):
        self._logger.debug("Setting up configured queues for exchange %s", exchange)
        for queue in queues:
            self._logger.debug("Declaring queue: %s", queue["name"])
            callback = functools.partial(
                self._on_queue_declare_ok, userdata=(queue, exchange)
            )
            self._channel.queue_declare(queue=queue["name"], callback=callback)

    def _on_queue_declare_ok(self, frame, userdata):
        queue, exchange = userdata
        self._logger.debug("Queue declared: %s", queue["name"])
        self._setup_bindings(queue, exchange)

    def _setup_bindings(self, queue, exchange):
        queue, bindings = queue["name"], queue.get("bindings", list())
        self._logger.debug("Setting up bindings for queue %s", queue)
        for binding in bindings:
            self._logger.debug("Declaring binding for queue: %s (%s)", queue, bindings)
            routing_key, arguments = binding.get("routing_key"), binding.get(
                "arguments"
            )
            callback = functools.partial(self._on_bind_ok, userdata=(binding, queue))
            self._channel.queue_bind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments,
                callback=callback,
            )

    def _on_bind_ok(self, frame, userdata):
        binding, queue = userdata
        self._logger.debug("Binding ok for queue: %s (%s)", queue, binding)
        self._start_consuming(queue)

    def _start_consuming(self, queue):
        self._logger.debug("Issuing consumer related RPC commands")
        callback = functools.partial(self._on_message, userdata=queue)
        consumer_tag = self._channel.basic_consume(queue, callback)
        self._consumer_tags.add(consumer_tag)
        if not self._consuming:
            # only add callback once
            self._logger.debug("Adding consumer cancellation callback")
            self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

        self._consuming = True
        self.was_consuming = True

    def _stop_consuming(self):
        if self._channel:
            consumers_copy = copy.deepcopy(self._consumer_tags)
            for consumer_tag in consumers_copy:
                self._logger.debug(
                    "Sending a Basic.Cancel RPC command to RabbitMQ for consumer %s",
                    consumer_tag,
                )
                callback = functools.partial(
                    self._on_consumer_cancelled_ok, userdata=consumer_tag
                )
                self._channel.basic_cancel(consumer_tag, callback)

    def _on_consumer_cancelled(self, frame):
        consumer_tag = frame.method.consumer_tag
        self._logger.warning("Consumer was cancelled remotely: %s", consumer_tag)
        self._consumer_tags.discard(consumer_tag)

    def _on_consumer_cancelled_ok(self, frame, userdata):
        self._logger.debug(
            "RabbitMQ acknowledged the cancellation of the consumer: %s", userdata
        )
        self._consumer_tags.discard(userdata)
        if not self._consumer_tags:
            # we either are shutting down or all consumers have been cancelled
            self._close_channel()

    def _on_message(self, channel, basic_deliver, properties, body, userdata):
        body = body.decode("utf-8")
        self._logger.debug("Received message for queue %s with body %s", userdata, body)

        body = self._deserialize_body(body)
        payload = {"queue": userdata, "body": body}

        try:
            self._dispatch(payload=payload)
        finally:
            self._channel.basic_ack(basic_deliver.delivery_tag)

    def _deserialize_body(self, body):
        if not self._deserialization_method:
            return body

        deserialization_func = self.DESERIALIZATION_FUNCTIONS[
            self._deserialization_method
        ]

        try:
            body = deserialization_func(body)
        except json.JSONDecodeError:
            pass

        return body
