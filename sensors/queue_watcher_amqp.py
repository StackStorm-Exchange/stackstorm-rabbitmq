"""
Author Notes:
Why does this sensor exist and uses asyncio rather than threading, multiprocessing, eventlet, etc?

Short Answer:
These concurrency methods all seem to suffer a similar issue: They do not work reliably when ran
inside the st2sensorcontainer wrapper while also in the st2sensorcontainer official docker image.
However in various other environments, they work fine.

(Very) Long Answer:
https://github.com/StackStorm/st2/discussions/5743
"""

import asyncio
import json
from copy import deepcopy
from typing import Any

import amqp
from st2reactor.sensor.base import Sensor

SENSOR_CONFIG_KEY = "amqp_watcher_sensor_config"
DEFAULT_RABBITMQ_PORT = 5672
DISAPTCH_TRIGGER_NAME = "rabbitmq.amqp_msg_rx"
DESERIALIZATION_FUNCTIONS: dict = {"json": json.loads}
RETRY_DELAY = 3  # seconds

# Default Queue Parameters by Queue type
DEFAULT_QUEUE_PARAMS_CLASSIC: dict = {
    "arguments": {"x-queue-type": "classic"},
    "auto_delete": True,
    "durable": False,
    "passive": False,
    "exclusive": False,
    "nowait": False,
}
DEFAULT_QUEUE_PARAMS_QUORUM: dict = {
    "arguments": {"x-queue-type": "quorum"},
    "auto_delete": False,
    "durable": True,
    "passive": False,
    "exclusive": False,
    "nowait": False,
}
DEFAULT_QUEUE_PARAMS_STREAM: dict = {
    "arguments": {"x-queue-type": "stream"},
    "auto_delete": False,
    "durable": True,
    "passive": False,
    "exclusive": False,
    "nowait": False,
}
DEFAULT_QUEUE_TYPE = "classic"
VALID_QUEUE_TYPES: dict = {
    "classic": DEFAULT_QUEUE_PARAMS_CLASSIC,
    "quorum": DEFAULT_QUEUE_PARAMS_QUORUM,
    "stream": DEFAULT_QUEUE_PARAMS_STREAM,
}

# Default Exchange Parameters by Exchange type
DEFAULT_EXCHANGE_PARAMS_DIRECT: dict = {
    "arguments": {},
    "type": "direct",
    "auto_delete": True,
    "durable": False,
    "passive": False,
    "nowait": False,
}
DEFAULT_EXCHANGE_PARAMS_FANOUT: dict = {
    "arguments": {},
    "type": "fanout",
    "auto_delete": True,
    "durable": False,
    "passive": False,
    "nowait": False,
}
DEFAULT_EXCHANGE_PARAMS_HEADERS: dict = {
    "arguments": {},
    "type": "headers",
    "auto_delete": True,
    "durable": False,
    "passive": False,
    "nowait": False,
}
DEFAULT_EXCHANGE_PARAMS_TOPIC: dict = {
    "arguments": {},
    "type": "topic",
    "auto_delete": True,
    "durable": False,
    "passive": False,
    "nowait": False,
}
DEFAULT_EXCHANGE_TYPE = "direct"
VALID_EXCHANGE_TYPES: dict = {
    "direct": DEFAULT_EXCHANGE_PARAMS_DIRECT,
    "fanout": DEFAULT_EXCHANGE_PARAMS_FANOUT,
    "headers": DEFAULT_EXCHANGE_PARAMS_HEADERS,
    "topic": DEFAULT_EXCHANGE_PARAMS_TOPIC,
}


class QueueWatcherAMQP(Sensor):
    """Sensor to watch queues for messages and dispatch trigger

    Defined Queues and Exchanges will be 'declared' on when sensor_wrapper runs setup()

    Documentation:
    py-amqp: https://docs.celeryq.dev/projects/amqp/en/latest/reference/
    asyncio: https://docs.python.org/3.8/whatsnew/3.8.html#asyncio (python version is important)
    """

    def __init__(self, sensor_service, config=None) -> None:
        super(QueueWatcherAMQP, self).__init__(
            sensor_service=sensor_service, config=config
        )
        self._logger = self.sensor_service.get_logger(name=self.__class__.__name__)
        self._dispatch = self.sensor_service.dispatch
        self.client = None
        self.sensor_config: dict = dict()

        self.sensor_config = config[SENSOR_CONFIG_KEY]
        self._logger.debug(f"Sensor config under key: {SENSOR_CONFIG_KEY} found")

        self.client = AMQPConnectionClient(
            logger=self._logger,
            callback=self._dispatch_trigger,
            **self.sensor_config,
        )

        self._logger.debug("__init__() Complete")

    def setup(self):
        self.client.setup()
        self._logger.debug("setup() Complete")

    def run(self):
        self._logger.debug("run(): Begin")

        while True:
            asyncio.run(self.client.consume_messages())

            # Sleep to hard prevent a run away loop
            self._logger.debug(f"sleeping for {RETRY_DELAY}s")
            asyncio.sleep(RETRY_DELAY)

        self._logger.debug("run(): Complete")

    def cleanup(self):
        self.client.conn.close()
        self._logger.debug("cleanup() Complete")

    def add_trigger(self, trigger):
        pass

    def update_trigger(self, trigger):
        pass

    def remove_trigger(self, trigger):
        pass

    def _dispatch_trigger(self, queue, body):
        """Pre-processing of data before dispatching trigger"""
        self._logger.debug(f"Dispatching trigger for Queue: {queue}")
        if isinstance(body, str):
            try:
                deserialize = DESERIALIZATION_FUNCTIONS["json"]
                body = deserialize(body)
            except Exception:
                # Ignore exceptions
                pass
        else:
            body = str(body)

        payload = {"queue": queue, "body": body}
        self._dispatch(trigger=DISAPTCH_TRIGGER_NAME, payload=payload)


class AMQPConnectionClient:
    """A class for managing the connection and activity with the server

    Can be called independently of stackstorm for testing.
    Reccomend to source the same virtualenv as the pack
    """

    def __init__(
        self,
        logger,
        callback: Any = None,
        host: str = str(),
        port: [int, None] = None,
        username: str = str(),
        password: str = str(),
        queues: [list, None] = None,
        exchanges: [list, None] = None,
    ):
        self._logger = logger
        self.host: str = host
        self.port: int = port if port else DEFAULT_RABBITMQ_PORT
        self.username: str = username
        self.password: str = password
        self.queues: list = queues
        self.exchanges: list = exchanges

        self.callback = callback
        self.conn = None
        self.src_bind: tuple = None
        self.channels: dict = dict()
        self.queue_channels: dict = dict()
        self.ctag: str = str()

        self._setup_conn()

    def setup(self):
        """Establish the connection, and declare Queues/Exchanges"""
        self._connect()

        # Declare Queues
        if self.queues:
            for queue_cfg in self.queues:
                self._declare_queue(**queue_cfg)

        # Declare Exchanges
        if self.exchanges:
            for exch_cfg in self.exchanges:
                self._declare_exchange(**exch_cfg)

                # Create Bindings for this exchange
                for bind in exch_cfg["bindings"]:
                    self._queue_bind(exchange=exch_cfg["exchange"], **bind)

    async def consume_messages(self):
        """The 'run' method for the class

        Begins consumming messages using the channel created for the declared queue
        Drains messages to the callback once consumed

        Uses asyncio coroutines to allow multiple queues to be watched simulatneously

        Why use async here?

            amqp.Connection().drain_events() would exit after drain is complete, and being
            ran in a forever loop allows the drain to never end. A problem occurs however
            since control is never released while waiting for an event to drain. This
            results in only 1 queue ever being able to drain at a time, and all others
            remain blocked (blindly) until the active queue has an event, drains, and
            cycles to the next queue to wait for an event (or drain)

            For this reason async was chosen as a simple way to overcome this issue, as
            well as the issue noted at the top of this file (the reason this sensor exists)
        """
        for queue, ch_id in self.queue_channels.items():
            # Passing None or empty str to channel.basic_consume(): parameter 'consumer_tag'
            # results in a new ctag being generated, returned, and assigned to self.ctag
            # Ensures one ctag per instance of this class
            self.ctag = self.channels[ch_id].basic_consume(
                queue=queue, callback=self._msg_callback, consumer_tag=self.ctag
            )

        while True:
            await self._drain_messages()

    async def _drain_messages(self):
        """amqp.Connection().drain_events()"""
        self.conn.drain_events()

    def _connect(self):
        """Actually establish a socket to the server"""
        host = f"{self.host}:{self.port}"

        self._logger.debug(f"Attempting to open connection to RabbitMQ @ {host}")
        self.conn.connect()
        self.src_bind = self.conn.sock.getsockname()

        local = f"{self.src_bind[0]}:{self.src_bind[1]}"
        self._logger.debug(
            f"Successfully connected to {host} from local binding {local}"
        )
        self.channels = self.conn.channels

    def _setup_conn(self):
        """Init amqp.Connection() and prepare the connection socket object"""
        host = f"{self.host}:{self.port}"
        self.conn = amqp.Connection(
            host=host, userid=self.username, password=self.password
        )
        self._logger.debug(
            f"Connection object created for {host} with user: {self.username}"
        )

    def _open_channel(self, channel_id: [int, None] = None):
        """Open a channel on this connection"""
        # When None is passed to .channel(), the server chooses the next available id automatically
        result = self.conn.channel(channel_id=channel_id)
        self._logger.debug(f"Channel opened with ID: {result.channel_id}")
        return result.channel_id

    def _declare_queue(self, queue: str, channel_id: [int, None] = None, **kwargs):
        """Declare a queue based on the provided parameters
        Note: **kwargs is transparently passing config keys form the pack config
        """
        queue_type = (
            kwargs["type"]
            if "type" in kwargs and kwargs["type"] in VALID_QUEUE_TYPES
            else DEFAULT_QUEUE_TYPE
        )

        # Opens a channel if the provided channel_id doesn't exist, or was None
        if not channel_id or not self.channels.get(channel_id, None):
            channel_id = self._open_channel(channel_id=channel_id)

        # Copy the defaults, overwrite the default if the kwarg key is a valid param
        # Extract default 'arguments', update custom params
        # Re-Add default arguments that weren't overwritten by custom params
        params = deepcopy(VALID_QUEUE_TYPES[queue_type])
        default_args = deepcopy(params["arguments"])
        params.update({k: v for k, v in kwargs.items() if k in params})
        params["arguments"].update(
            {k: v for k, v in default_args.items() if k not in params["arguments"]}
        )

        self._logger.info(f"Declaring Queue: {queue}")

        self.channels[channel_id].queue_declare(queue=queue, **params)
        self.queue_channels.update({queue: channel_id})

        self._logger.debug(f"Declared queue: {queue} with params: {params}")

    def _declare_exchange(self, exchange: str, channel_id: int = 1, **kwargs):
        """Declare an exchange based on the provided parameters
        Note: **kwargs is transparently passing config keys form the pack config
        """
        exchange_type = (
            kwargs["type"]
            if "type" in kwargs and kwargs["type"] in VALID_EXCHANGE_TYPES
            else DEFAULT_EXCHANGE_TYPE
        )

        # Copy the defaults, overwrite the default if the kwarg key is a valid param
        params = deepcopy(VALID_EXCHANGE_TYPES[exchange_type])
        params.update({k: v for k, v in kwargs.items() if k in params})

        self._logger.info(f"Declaring Exchange: {exchange}")

        self.channels[channel_id].exchange_declare(exchange=exchange, **params)

        self._logger.debug(f"Declared exchange: {exchange} with params: {params}")

    def _queue_bind(
        self,
        exchange: str,
        queue: str,
        routing_key: str,
        arguments: dict = dict(),
        channel_id: int = 1,
    ):
        """Bind a queue to an exchange based on provided parameters"""
        self._logger.debug(
            f"Binding queue: {queue} to exchange: {exchange} with routing key: {routing_key}"
        )

        self.channels[channel_id].queue_bind(
            queue=queue, exchange=exchange, routing_key=routing_key, arguments=arguments
        )

    def _get_queue_by_ch_id(self, ch_id: int):
        """Look up a queue name by a known channel_id"""
        search = self.queue_channels
        return list(search.keys())[list(search.values()).index(ch_id)]

    def _msg_callback(self, message):
        """Pre-Processes message details before returning to the call back function
        If no callback was provided, the message will print to console and log (for testing)
        """
        body = message.body.decode("utf-8")
        ctag = message.delivery_info["consumer_tag"]
        ch_id = message.channel.channel_id
        queue = self._get_queue_by_ch_id(ch_id)

        self._logger.info(f"Received message in {queue} for consumer {ctag}")

        if self.callback:
            self.callback(queue, body)
        else:
            # Will never happen when called within st2senorcontainer
            log_msg = f"_msg_callback(): Message Received ({queue}) {body}"
            print(log_msg)
            self._logger.info(log_msg)

        self.channels[ch_id].basic_ack(message.delivery_tag)
