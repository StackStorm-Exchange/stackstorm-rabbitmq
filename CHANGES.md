# Change Log

# 1.2.0
- Added `RabbitMQBoundQueueSensor` to support advanced routing options for queues

# 1.1.1
- Updated pip dependency to pika `1.3.x` to support python >= 3.7

# 1.1.0
- Added key to sensor config schema to identify a queue as type: quorum
- Updated setup() to understand how to create/load queues with types: ["classic", "quorum"]

# 1.0.0

* Drop Python 2.7 support

# 0.5.3

- Fixed bug in `queues_sensor` where the parameter `body` was being returned as `byte` and not a `string`

  Contributed by Rick Kauffman (@netwookie wookieware.com)

# 0.5.2

- Fixed bug in `queues_sensor` where the channel wasn't calling `basic_consume` with the correct arguments
- Fixed bug in `queues sensor` where the trigger type of `rabbitmq.new_message` had an incorrect type of `object` for the parameter `body` when instead it should have be a `string`.

  Contributed by Nick Maludy (@nmaludy Encore Technologies)

# 0.5.0

- Updated to pika 0.11.x, updated exchange\_type parameter, import re-ordering

# 0.4.2

- Minor linting

# 0.4.0

- Fixed trigger.queue incorrect

# 0.3.0

- Fixed broken `list_exchanges` and `list_queues` actions
- Note that format of `list_queues` output has changed, due to RabbitMQ changes

# 0.2.0

- Rename `config.yaml` to `config.schema.yaml` and update to use schema.

# 0.1.0

- First release
