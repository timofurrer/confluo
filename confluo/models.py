"""
    `confluo` - Minimalist scalable microservice framework for distributed systems using AMQP/RabbitMQ

    This module contains all required models to run a service.
    The models in this module are:
        - Command
        - Response
        - Event

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import json
import asyncio

from .errors import ServiceError


class ProtocolModel:
    """Base class for all models which are used
    in a AMQP message.
    """
    def __init__(self, **kwargs):
        #: Holds all fields for this model
        self.values = kwargs

    @classmethod
    def loads(cls, payload):
        """Load a command from an AMQP message payload.

        :param str payload: a received message payload.

        :returns: a Command instance
        :rtype: Command
        """
        payload_data = json.loads(payload.decode("utf-8"))

        # raises exception if data is invalid.
        field_values = cls.verify_data(payload_data)

        # create command instance.
        return cls(**field_values)

    @classmethod
    def verify_data(cls, payload_data):
        """Verify if a received AMQP message payload contains the
        minimum required attributes for a Command.

        :param dict payload_data: the recevied payload data.

        :raises ServiceError: if given payload data is invalid.
        """
        field_values = {}
        try:
            for field in cls.FIELDS:
                field_values[field] = payload_data[field]
        except KeyError as e:
            raise ServiceError("Missing '{0}' field in message.".format(e))
        else:
            return field_values

    def __str__(self):
        """Return the command as an AMQP payload."""
        return json.dumps(self.values)

    def __getattr__(self, attr):
        """Get a specific value from the model.

        :param str attr: the name of the field.
        """
        return self.values[attr]


class Command(ProtocolModel):
    """Represents an AMQP RPC command message.

    :param str path: the destination path for this command.
    :param dict query: the query data for this command.
    :param dict body: the body for this command.
    :param dict headers: the headers for this command.
    """
    FIELDS = ["path", "query", "body", "headers"]

    def __init__(self, path, query, body, headers=None):
        super().__init__(path=path, query=query, body=body, headers=headers)


class Response(ProtocolModel):
    """Represents an AMQP RPC response message.

    :param str path: the destination path for this response.
    :param dict body: the body for this response.
    :param dict headers: the headers for this response.
    """
    FIELDS = ["path", "body", "status_code", "headers"]

    def __init__(self, path, body, status_code=200, headers=None):
        super().__init__(path=path, body=body, status_code=status_code, headers=headers)


class Event(ProtocolModel):
    """Represents an AMQP RPC event message.

    :param str path: the destination path for this event.
    :param dict body: the body for this event.
    :param dict headers: the headers for this event.
    """
    FIELDS = ["path", "body", "headers"]

    def __init__(self, path, body, headers=None):
        super().__init__(path=path, body=body, headers=headers)


class CommandTransaction:
    """Represents a Command transaction.

    It contains the received Response and
    an Event object.
    """
    def __init__(self):
        #: Holds the event object
        self.event = asyncio.Event()

        #: Holds the received Response for the Command.
        self.response = None
