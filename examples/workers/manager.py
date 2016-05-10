"""
    Example service for confluo.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import asyncio
import logging

from confluo.core import Service


__BROKER_HOST__ = "172.17.0.2"

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    manager = Service("Manager", loop=loop)

    # logging.basicConfig(level=logging.DEBUG)

    # connect services
    loop.run_until_complete(manager.connect(__BROKER_HOST__))
    loop.run_until_complete(manager.call("Worker", "/calculate", {"data": "please calculate this"}, expect_response=False))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(manager.shutdown())
        loop.stop()
        loop.close()
