# confluo

> From **con-** ‎(“with; together”) + **fluō** ‎(“flow”). <br>
> **cōnfluō** ‎(present infinitive cōnfluere, perfect active cōnfluxī); third conjugation, no passive <br>
> &nbsp;&nbsp;&nbsp;&nbsp;1. (intransitive) I flow or run together, meet. <br>
> &nbsp;&nbsp;&nbsp;&nbsp;2. (intransitive, figuratively) I flock or crowd together, throng, assemble. <br>

⇢ **confluo** is a minimalist scalable distributed microservice framework using AMQP/RabbitMQ as a broker.

```python
import asyncio
from confluo.core import Service

a = Service("Service-A")
b = Service("Service-B")


@b.subscribe("/foo/bar")
async def foo_bar_event(path, headers, body):
    print("Got /foo/bar event with body: '{0}'".format(body))
    response = await b.call("Service-A", "/bar", {"data": "Some body data"})
    print("Got response: '{0}'".format(response))


@a.route("/bar")
async def bar_command(path, headers, body):
    print("Got /bar command with body: '{0}'".format(body))
    return {"value": 1}


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    # connect services
    loop.run_until_complete(asyncio.wait([a.connect(), b.connect()]))

    # Publish event
    loop.run_until_complete(a.publish("/foo/bar", "wtf"))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(asyncio.wait([a.shutdown(), b.shutdown()]))
        loop.stop()
        loop.close()

```

## Usage

This section walks through the essentials in order to run a **confluo** service.

### Run RabbitMQ as a broker.

The simplest way to use RabbitMQ is probably to just pull the docker image from docker hub and run it:

```bash
sudo docker run -d --hostname my-rabbit --name some-rabbit rabbitmq:3-management
```

### Import and create a service

Let's create our first service named *My-First-Service* and bind it to the main threads asyncio event loop:

```python
import asyncio
from confluo import Service

loop = asyncio.get_event_loop()
service = Service("My-First-Service", loop=loop)
```

### Publish event

Every *confluo service* is able to publish events. All other services which subscribed to this event will receive it.

```python
loop.run_until_complete(service.publish("MyFancyEvent", {"name": "Peter", "nickname": "Spider Man"}))
```

The first argument of the `Service.publish` method is the name of the event. You can also use a path like `/my/cool/path//event` or whatever you like. The second argument is the data sent as event body.

### Subscribe event

Use the `Service.subscribe` decorator to subscribe for an event and register a handler:

```python
@service.subscribe("MyFancyEvent")
async def handle_my_fancy_event(path, headers, body):
    print("Got MyFancyEvent with body: {0}".format(body))
```
