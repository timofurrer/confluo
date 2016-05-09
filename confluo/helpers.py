"""
    `confluo` - Minimalist scalable microservice framework for distributed systems using AMQP/RabbitMQ

    This module contains helper functions for the confluo service.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

def path_to_routing_key(path):
    """Convert a path to a AMQP valid routing key.

    It replaces all slashes (/) with dots (.)
    and removes the first slash.

    :param str path: the path to convert to a routing key.
    """
    if path.startswith("/"):
        path = path[1:]
    return path.replace("/", ".")
