"""
    `skynet-service` - Microservice base for a component in skynet.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import json
import uuid
import asyncio
import aioamqp
import logging

from .errors import ServiceError


class Service:
    """Represents a skynet microservice component.

    :param str name: the name of this skynet service.
    """
    def __init__(self, name, loop):
        #: Holds the name of this skynet service.
        self.name = name

        #: Holds the asyncio loop
        self.loop = loop

        #: Holds the service logger
        self.logger = logging.getLogger(self.name)

        #: Holds the RPC exchange name
        self.rpc_exchange_name = "rpc"

        #: Holds the event exchange name
        self.event_exchange_name = "events"

        #: Holds the name of the command queue
        self.command_queue_name = "{0}".format(self.name)

        #: Holds the name of the response queue
        self.response_queue_name = "{0}-responses".format(self.name)

        #: Holds the name of the event queue
        self.event_queue_name = "{0}-events".format(self.name)

        #: Holds the aioamqp protocol
        self.command_protocol = None
        self.response_protocol = None
        self.event_protocol = None

        #: Holds the command channel
        self.command_channel = None

        #: Holds the response channel
        self.response_channel = None

        #: Holds the event channel
        self.event_channel = None

        #: Holds all registered command handlers.
        self.command_routes = {}

        #: Holds all registered event handlers.
        self.event_routes = {}

        #: Holds all command/response transactions.
        self.command_transactions = {}

    async def connect(self, broker):
        """Connects to the given broker.

        All protocol, channel and queues are setup and
        prepared for communication.

        :param str broker: the ip address or hostname of the broker to use.
                           This must be an AMQP broker like RabbitMQ.
        """
        # setup connection to the command channel and queue.
        _, self.command_protocol = await aioamqp.connect(broker)
        self.command_channel = await self.command_protocol.channel()

        await self.command_channel.exchange_declare(self.rpc_exchange_name, type_name="direct", durable=True)
        await self.command_channel.queue_declare(self.command_queue_name, durable=True)
        await self.command_channel.queue_bind(self.command_queue_name, exchange_name=self.rpc_exchange_name, routing_key=self.command_queue_name)
        await self.command_channel.basic_consume(self._on_command, queue_name=self.command_queue_name)

        # setup connection to the response channel and queue.
        _, self.response_protocol = await aioamqp.connect(broker)
        self.response_channel = await self.response_protocol.channel()

        await self.response_channel.queue_declare(self.response_queue_name, durable=True, exclusive=True)
        await self.response_channel.queue_bind(self.response_queue_name, exchange_name=self.rpc_exchange_name, routing_key="")
        await self.response_channel.basic_consume(self._on_response, queue_name=self.response_queue_name)

        # setup connection to the commands channel and queue.
        _, self.event_protocol = await aioamqp.connect(broker)
        self.event_channel = await self.event_protocol.channel()

        await self.event_channel.exchange_declare(self.event_exchange_name, type_name="topic", durable=True)
        # TODO: might want to set exclusive=True
        await self.event_channel.queue_declare(self.event_queue_name, durable=True)
        await self.event_channel.basic_consume(self._on_event, queue_name=self.event_queue_name)

        # subscribe to all registered event routes.
        for route in self.event_routes:
            await self.event_channel.queue_bind(self.event_queue_name, exchange_name=self.event_exchange_name, routing_key=route)

    async def _on_command(self, channel, body, envelope, properties):
        """Handle a received command.

        All messages received on the ``command queue`` are handled and
        dispatched in this method.

        :param channel: the channel on which the message was received.
        :param body: the body of the message which was received.
        :param envelope: the metadata about the message which was received.
        :param properties: the AMQP properties of the message which was received.
        """
        # deserialize the AMQP message body.
        message = json.loads(body.encode("utf-8"))

        # verify that there are all required fields in the message.
        try:
            path = message["path"]
            query = message["query"]
            headers = message["headers"]
            body = message["body"]
        except KeyError as e:
            # TODO: report to caller
            self.logger.warning("Missing '{0}' field in message.".format(e))
            return

        try:
            func = self.command_routes[path]
        except Keyerror:
            # TODO: report to caller / or just ignore?!
            self.logger.warning("No route for path '{0}' defined.".format(path))
            return

        # call command handler and wait for response.
        response = await func(path, query, headers, body)
        if not properties.reply_to:
            # no response is required - just ignore the response given by the command handler.
            return

        # send response to caller
        await self.command_channel.basic_publish(
            payload=json.dumps(response),
            exchange_name=self.rpc_exchange_name,
            routing_key=properties.reply_to,
            properties={
                "correlation_id": properties.correlation_id
            })

        # send acknowledge for this command message.
        await self.command_channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def _on_response(self, channel, body, envelope, properties):
        """Handle a received response.

        All messages received on the ``response queue`` are handled
        here. It basically sets the response data and triggers
        an event which stops the caller to wait for the response.

        :param channel: the channel on which the message was received.
        :param body: the body of the message which was received.
        :param envelope: the metadata about the message which was received.
        :param properties: the AMQP properties of the message which was received.
        """
        # check if this service is waiting for the received response.
        try:
            transaction = self.command_transactions[properties.correlation_id]
        except KeyError:
            self.logger.warning("Received martian response for message: {0}".format(
                properties.correlation_id))
            return

        # deserialiye the AMQP message body.
        message = json.loads(body.encode("utf-8"))

        # verify that there are all required fields in the message.
        try:
            headers = message["headers"]
            status_code = message["status_code"]
            body = message["body"]
        except KeyError as e:
            # TODO: report to caller
            self.logger.warning("Missing '{0}' field in message.".format(e))
            return

        # set response message and trigger event.
        transaction["message"] = headers, status_code, body
        transaction["event"].set()

    async def _on_event(self, channel, body, envelope, properties):
        """Handle a received event.

        All messages received on the ``event queue`` are handled and
        dispatched in this method.

        :param channel: the channel on which the message was received.
        :param body: the body of the message which was received.
        :param envelope: the metadata about the message which was received.
        :param properties: the AMQP properties of the message which was received.
        """
        # deserialize the AMQP message body.
        message = json.loads(body.encode("utf-8"))

        # verify that there are all required fields in the message.
        try:
            path = message["path"]
            headers = message["headers"]
            body = message["body"]
        except KeyError as e:
            # TODO: report to caller
            self.logger.warning("Missing '{0}' field in message.".format(e))
            return

        try:
            func = self.event_routes[path]
        except Keyerror:
            # TODO: report to caller / or just ignore?!
            self.logger.warning("No route for path '{0}' defined.".format(path))
            return

        # call event handler.
        await func(path, query, headers, body)

    async def call(self, service_name, path, body, query=None, headers=None, timeout=20.0, expect_response=True):
        """Call a command on a specific type of service.

        :param str service_name: the name of the destination service.
        :param str path: the path of the command.
        :param dict body: the body of the message to send.
        :param dict query: optional path query data.
        :param dict: headers: optional header data.
        :param float timeout: the timeout to wait for a response
        :paran bool expect_response: flag if a response is expected or not.

        :returns: the response of the command call if expected or nothing.
        :rtype: tuple
        """
        message = {
            "path": path, "query": query or {},
            "headers": headers or {}, "body": body
        }
        message_id = str(uuid.uuid4())

        properties={}
        event = None
        if expect_response:
            properties["reply_to"] = self.response_queue_name
            properties["correlation_id"] = message_id

            # register command/response transaction
            event = asyncio.Event()
            transaction = {"event": event, "message": None}
            self.command_transactions[message_id] = transaction

        # send command to the rpc exchange
        await self.command_channel.basic_publish(
            payload=json.dumps(message),
            exchange_name=self.rpc_exchange_name,
            routing_key=service_name,
            properties=properties
        )

        # no response is expected.
        if not expect_response:
            return

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            self.logger.error("No response received for message '{0}' within {1} seconds.".format(
                message_id, timeout))
            raise

        return transaction["message"]

    def route(self, path):
        """Register to a command sent with the given path.

        This method should be used as a decorator.

        :param str path: the path of the command to register to.
        """
        def decorator(func):
            """The route decorator."""
            self.command_routes[path] = func
            return func
        return decorator

    async def publish(self, path, body, headers=None):
        """Publish an event with a specific path, body and headers.

        :param str path: the path to publish the event.
        :param dict body: the body of the event.
        :param dict headers: optional headers of the event.
        """
        message = {
            "path": path, "headers": headers or {},
            "body": body
        }

        await self.event_channel.basic_publish(
            payload=message,
            exchange_name=self.event_exchange_name,
            routing_key=path.replace("/", ".")
        )

    def subscribe(self, path):
        """Subscribe to an event published with the given path.

        This method should be used as a decorator.

        :param str path: the path of events to subscribe to.
        """
        def decorator(func):
            """The subscribe decorator."""
            self.event_routes[path] = func
            return func
        return decorator
