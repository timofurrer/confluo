"""
    `confluo` - Minimalist scalable microservice framework for distributed systems using AMQP/RabbitMQ

    This module contains the confluo service class for a component in a confluo network.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import json
import uuid
import asyncio
import aioamqp
import logging

from .models import Command, Response, Event, CommandTransaction
from .errors import ServiceError
from .helpers import path_to_routing_key


class Service:
    """Represents a confluo microservice component.

    :param str name: the name of this confluo service.
    :param asyncio.BaseEventLoop loop: the event loop to use to run this service.
                                       If no event loop is given the ``asyncio.get_event_loop``
                                       is used.
    :param logging.Logger logger: the logger instance to use for this service.
                                  If no logger is given a new ``logging.getLogger()`` is created.
    """
    def __init__(self, name, loop=None, logger=None):
        #: Holds the name of this confluo service.
        self.name = name

        #: Holds the asyncio loop
        self.loop = loop or asyncio.get_event_loop()

        #: Holds the service logger
        self.logger = logger or logging.getLogger(self.name)

        #: Holds the RPC exchange name
        self.rpc_exchange_name = "rpc"

        #: Holds the event exchange name
        self.event_exchange_name = "events"

        #: Holds the name of the command queue
        self.command_queue_name = "{0}".format(self.name)

        #: Holds the name of the response queue
        self.response_queue_name = "{0}-responses-{1}".format(self.name, str(uuid.uuid4()))

        #: Holds the name of the event queue
        self.event_queue_name = "{0}-events".format(self.name)

        #: Holds the aioamqp transport layers
        self.command_transport = None
        self.response_transport = None
        self.event_transport = None

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

    async def connect(self, broker="localhost"):
        """Connects to the given broker.

        All protocol, channel and queues are setup and
        prepared for communication.

        :param str broker: the ip address or hostname of the broker to use.
                           This must be an AMQP broker like RabbitMQ.
        """
        # setup connection to the command channel and queue.
        self.command_transport, self.command_protocol = await aioamqp.connect(broker)
        self.command_channel = await self.command_protocol.channel()

        await self.command_channel.exchange_declare(self.rpc_exchange_name, type_name="direct", durable=True)
        await self.command_channel.queue_declare(self.command_queue_name, durable=True)
        await self.command_channel.queue_bind(self.command_queue_name, exchange_name=self.rpc_exchange_name, routing_key=self.command_queue_name)
        await self.command_channel.basic_consume(self._on_command, queue_name=self.command_queue_name, no_ack=True)

        self.logger.debug("Connected to command channel and created queue %s.", self.command_queue_name)

        # setup connection to the response channel and queue.
        self.response_transport, self.response_protocol = await aioamqp.connect(broker)
        self.response_channel = await self.response_protocol.channel()

        await self.response_channel.exchange_declare(self.rpc_exchange_name, type_name="direct", durable=True)
        # await self.response_channel.queue_declare(self.response_queue_name, durable=True, exclusive=True)
        await self.response_channel.queue_declare(self.response_queue_name, exclusive=True)
        await self.response_channel.queue_bind(self.response_queue_name, exchange_name=self.rpc_exchange_name, routing_key=self.response_queue_name)
        await self.response_channel.basic_consume(self._on_response, queue_name=self.response_queue_name, no_ack=True)

        self.logger.debug("Connected to response channel and created queue %s.", self.response_queue_name)

        # setup connection to the commands channel and queue.
        self.event_transport, self.event_protocol = await aioamqp.connect(broker)
        self.event_channel = await self.event_protocol.channel()

        await self.event_channel.exchange_declare(self.event_exchange_name, type_name="topic", durable=True)
        await self.event_channel.queue_declare(self.event_queue_name, durable=True)
        await self.event_channel.basic_consume(self._on_event, queue_name=self.event_queue_name, no_ack=True)

        self.logger.debug("Connected to event channel and created queue %s.", self.event_queue_name)

        # subscribe to all registered event routes.
        for route in self.event_routes:
            routing_key = path_to_routing_key(route)
            await self.event_channel.queue_bind(self.event_queue_name, exchange_name=self.event_exchange_name, routing_key=routing_key)
            self.logger.debug("Subscribed to event %s on event channel %s.", routing_key, self.event_channel)

    async def _on_command(self, channel, body, envelope, properties):
        """Handle a received command.

        All messages received on the ``command queue`` are handled and
        dispatched in this method.

        :param channel: the channel on which the message was received.
        :param body: the body of the message which was received.
        :param envelope: the metadata about the message which was received.
        :param properties: the AMQP properties of the message which was received.
        """
        self.logger.debug("Received Command '%s' in message '%s'", body, properties.message_id)
        # create a command instance from AMQP message body.
        command = Command.loads(body)

        try:
            func = self.command_routes[command.path]
        except KeyError:
            # TODO: report to caller / or just ignore?!
            self.logger.warning("No route for path '%s' defined.", command.path)
            return

        # call command handler and wait for response.
        response = await func(command.path, command.query, command.headers, command.body)
        if not properties.reply_to:
            # no response is required - just ignore the response given by the command handler.
            self.logger.debug("Do not send response for Command %s because no reply_to is given.", properties.message_id)
            return

        if not isinstance(response, Response):
            if isinstance(response, tuple):
                body, status_code, headers = response
            else:
                body = response
                status_code = 200
                headers = None

            response = Response(command.path, body, status_code, headers)

        # send response to caller
        await channel.basic_publish(
            payload=str(response),
            exchange_name=self.rpc_exchange_name,
            routing_key=properties.reply_to,
            properties={
                "correlation_id": properties.correlation_id
            })

        self.logger.debug("Sent response '%s' for Command '%s'.", response, properties.message_id)

        # send acknowledge for this command message.
        # FIXME: why don't I get the response if I ack the message?!
        # await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

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
        self.logger.debug("Received Response '%s' for Command '%s'.", body, properties.correlation_id)
        # check if this service is waiting for the received response.
        try:
            transaction = self.command_transactions[properties.correlation_id]
        except KeyError:
            self.logger.warning("Received martian response for message: %s",
                properties.correlation_id)
            return

        # deserialiye the AMQP message body.
        response = Response.loads(body)

        # set response message and trigger event.
        transaction.response = response
        transaction.event.set()

    async def _on_event(self, channel, body, envelope, properties):
        """Handle a received event.

        All messages received on the ``event queue`` are handled and
        dispatched in this method.

        :param channel: the channel on which the message was received.
        :param body: the body of the message which was received.
        :param envelope: the metadata about the message which was received.
        :param properties: the AMQP properties of the message which was received.
        """
        self.logger.debug("Received Event '%s'.", body)

        # deserialize the AMQP message body.
        event = Event.loads(body)

        try:
            func = self.event_routes[event.path]
        except KeyError:
            # TODO: report to caller / or just ignore?!
            self.logger.warning("No route for path '%s' defined.", event.path)
            return

        # call event handler.
        await func(event.path, event.headers, event.body)

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
        command = Command(path, query, body, headers)
        message_id = str(uuid.uuid4())

        properties={}
        if expect_response:
            properties["reply_to"] = self.response_queue_name
            properties["correlation_id"] = message_id

            # register command/response transaction
            transaction = CommandTransaction()
            self.command_transactions[message_id] = transaction

        # send command to the rpc exchange
        await self.command_channel.basic_publish(
            payload=str(command),
            exchange_name=self.rpc_exchange_name,
            routing_key=service_name,
            properties=properties
        )

        # no response is expected.
        if not expect_response:
            self.logger.debug("Sent Command '%s' to '%s' and do not expect Response.",
                              command, service_name)
            return

        self.logger.debug("Sent Command '%s' to '%s' and wait for Response on '%s'.",
                          command, service_name, properties["reply_to"])

        try:
            await asyncio.wait_for(transaction.event.wait(), timeout)
        except asyncio.TimeoutError:
            self.logger.error("No response received for message '%s' within %s seconds.",
                message_id, timeout)
            raise
        else:
            return transaction.response
        finally:
            del self.command_transactions[message_id]

    def route(self, path):
        """Register to a command sent with the given path.

        This method should be used as a decorator.

        :param str path: the path of the command to register to.
        """
        def decorator(func):
            """The route decorator."""
            if path in self.command_routes:
                raise ServiceError("Command route with path '{0}' already registered.".format(path))
            self.command_routes[path] = func
            return func
        return decorator

    async def publish(self, path, body, headers=None):
        """Publish an event with a specific path, body and headers.

        :param str path: the path to publish the event.
        :param dict body: the body of the event.
        :param dict headers: optional headers of the event.
        """
        event = Event(path, body, headers)

        await self.event_channel.basic_publish(
            payload=str(event),
            exchange_name=self.event_exchange_name,
            routing_key=path_to_routing_key(path)
        )

        self.logger.debug("Published event '%s' for '%s'.", event, path)

    def subscribe(self, path):
        """Subscribe to an event published with the given path.

        This method should be used as a decorator.

        :param str path: the path of events to subscribe to.
        """
        def decorator(func):
            """The subscribe decorator."""
            if path in self.event_routes:
                raise ServiceError("Event route with path '{0}' already registered.".format(path))
            self.event_routes[path] = func
            return func
        return decorator

    def run(self, broker="localhost"):
        """Run this Service in the configured event loop.

        This method should only be used if you want to use
        ``run_forever`` and this is the only Service which should run.

        :param str broker: the AMQP broker to connect to.
        """
        # connect services
        self.loop.run_until_complete(self.connect(broker))

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            self.loop.run_until_complete(self.shutdown())
            self.loop.stop()
            self.loop.close()
            raise

    async def shutdown(self):
        """Shutdown all open AMQP protocols and transports.

        This method should be called before stopping the event loop.
        """
        # close command protocol and transport
        await self.command_protocol.close()
        self.command_transport.close()
        self.logger.debug("Closed command protocol and transport layer.")

        # close response protocol and transport
        await self.response_protocol.close()
        self.response_transport.close()
        self.logger.debug("Closed response protocol and transport layer.")

        # close event protocol and transport
        await self.event_protocol.close()
        self.event_transport.close()
        self.logger.debug("Closed even nrotocol and transport layer.")
