"""
    `confluo` - Minimalist scalable microservice framework for distributed systems using AMQP/RabbitMQ

    This module contains all confluo specific exceptions.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

class ServiceError(Exception):
    """Exception which is raised for confluo specific errors."""
