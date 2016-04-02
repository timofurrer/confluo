"""
    `skynet-service` - Microservice base for a component in skynet.

    Example service for skynet.

    :copyright: (c) by Timo Furrer
    :license: MIT, see LICENSE for details
"""

import asyncio
from service import Service


__BROKER_HOST__ = "172.17.0.2"

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    a = Service("A", loop=loop)
    b = Service("B", loop=loop)

    @b.subscribe("/foo/bar")
    async def foo_bar(path, headers, body):
        print("Path:", path)
        print("Headers:", headers)
        print("Body:", body)
        print("Send cmd to service A")
        response = await b.call("A", "/first/cmd", {"data": "Some body data"})
        print("Got response {0}".format(response))

    @a.route("/first/cmd")
    async def first_cmd(path, query, headers, body):
        print("Path:", path)
        print("Query:", query)
        print("Headers:", headers)
        print("Body:", body)

        return {"data": "Some data"}

    # connect services
    loop.run_until_complete(asyncio.wait([a.connect(__BROKER_HOST__), b.connect(__BROKER_HOST__)]))

    loop.run_until_complete(a.publish("/foo/bar", "wtf"))
    loop.run_forever()
