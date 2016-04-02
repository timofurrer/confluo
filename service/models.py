"""
    `skynet-service` - Microservice base for a component in skynet.

    This module contains all required models to run a service.
    The models in this module are:
        - Command
        - Response
        - Event

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import json

from .errors import ServiceError


class Command:
    """Represents an AMQP RPC command message.

    :param str path: the destination path for this command.
    :param dict query: the query data for this command.
    :param dict body: the body for this command.
    :param dict headers: the headers for this command.
    """
    def __init__(self, path, query, body, headers=None):
        #: Holds the destination path for this command.
        self.path = path

        #: Holds the query for this command.
        self.query = query

        #: Holds the body of this command.
        self.body = body or {}

        #: Holds the headers of this command.
        self.headers = headers or {}

    @classmethod
    def loads(cls, payload):
        """Load a command from an AMQP message payload.

        :param str payload: a received message payload.

        :returns: a Command instance
        :rtype: Command
        """
        payload_data = json.loads(payload.decode("utf-8"))

        # raises exception if data is invalid.
        path, query, headers, body = cls.verify_data(payload_data)

        # create command instance.
        return cls(path, query, body, headers)


    @staticmethod
    def verify_data(payload_data):
        """Verify if a received AMQP message payload contains the
        minimum required attributes for a Command.

        :param dict payload_data: the recevied payload data.

        :raises ServiceError: if given payload data is invalid.
        """
        try:
            path = payload_data["path"]
            query = payload_data["query"]
            headers = payload_data["headers"]
            body = payload_data["body"]
        except KeyError as e:
            raise ServiceError("Missing '{0}' field in message.".format(e))
        else:
            return path, query, headers, body

    def __str__(self):
        """Return the command as an AMQP payload."""
        payload = dict(
            path=self.path, query=self.query,
            body=self.body, headers=self.headers
        )
        return json.dumps(payload)


class Response:
    """Represents an AMQP RPC response message.

    :param str path: the destination path for this response.
    :param dict body: the body for this response.
    :param dict headers: the headers for this response.
    """
    def __init__(self, path, body, headers=None):
        #: Holds the destination path for this command.
        self.path = path

        #: Holds the body of this command.
        self.body = body or {}

        #: Holds the headers of this command.
        self.headers = headers or {}

    @classmethod
    def loads(cls, payload):
        """Load a response from an AMQP message payload.

        :param str payload: a received message payload.

        :returns: a Response instance
        :rtype: Response
        """
        payload_data = json.loads(payload.decode("utf-8"))

        # raises exception if data is invalid.
        path, headers, body = cls.verify_data(payload_data)

        # create response instance.
        return cls(path, body, headers)


    @staticmethod
    def verify_data(payload_data):
        """Verify if a received AMQP message payload contains the
        minimum required attributes for a Response.

        :param dict payload_data: the recevied payload data.

        :raises ServiceError: if given payload data is invalid.
        """
        try:
            path = payload_data["path"]
            headers = payload_data["headers"]
            body = payload_data["body"]
        except KeyError as e:
            raise ServiceError("Missing '{0}' field in message.".format(e))
        else:
            return path, headers, body

    def __str__(self):
        """Return the response as an AMQP payload."""
        payload = dict(
            path=self.path,
            body=self.body, headers=self.headers
        )
        return json.dumps(payload)


class Event:
    """Represents an AMQP RPC event message.

    :param str path: the destination path for this event.
    :param dict body: the body for this event.
    :param dict headers: the headers for this event.
    """
    def __init__(self, path, body, headers=None):
        #: Holds the destination path for this command.
        self.path = path

        #: Holds the body of this command.
        self.body = body or {}

        #: Holds the headers of this command.
        self.headers = headers or {}

    @classmethod
    def loads(cls, payload):
        """Load a response from an AMQP message payload.

        :param str payload: a received message payload.

        :returns: a Response instance
        :rtype: Response
        """
        payload_data = json.loads(payload.decode("utf-8"))

        # raises exception if data is invalid.
        path, headers, body = cls.verify_data(payload_data)

        # create response instance.
        return cls(path, body, headers)


    @staticmethod
    def verify_data(payload_data):
        """Verify if a received AMQP message payload contains the
        minimum required attributes for an Event.

        :param dict payload_data: the recevied payload data.

        :raises ServiceError: if given payload data is invalid.
        """
        try:
            path = payload_data["path"]
            headers = payload_data["headers"]
            body = payload_data["body"]
        except KeyError as e:
            raise ServiceError("Missing '{0}' field in message.".format(e))
        else:
            return path, headers, body

    def __str__(self):
        """Return the event as an AMQP payload."""
        payload = dict(
            path=self.path,
            body=self.body, headers=self.headers
        )
        return json.dumps(payload)
