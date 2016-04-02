"""
    `skynet-service` - Microservice base for a component in skynet.

    Example service for skynet.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import asyncio
import logging

from service import Service


__BROKER_HOST__ = "172.17.0.2"

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    worker = Service("Worker", loop=loop)

    # logging.basicConfig(level=logging.DEBUG)

    @worker.route("/calculate")
    async def calculate(path, query, headers, body):
        print("Calculating for {0}".format(body))
        await asyncio.sleep(5)
        print("Calculated!!")

    # connect services
    loop.run_until_complete(worker.connect(__BROKER_HOST__))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(worker.shutdown())
        loop.stop()
        loop.close()
