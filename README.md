A simple PBFT protocol over HTTP, using python3 asyncio/aiohttp. This is just a proof of concept implementation.

## Configuration
A `pbft.yaml` config file is needed for the nodes to run. A sample is as follows:
```Yaml
nodes:
    - host: localhost
      port: 30000
    - host: localhost
      port: 30001
    - host: localhost
      port: 30002
    - host: localhost
      port: 30003

clients:
    - host: localhost
      port: 20001
    - host: localhost
      port: 20002

loss%: 0

ckpt_interval: 10

retry_times_before_view_change: 2

sync_interval: 5

misc:
    network_timeout: 5
```

## Run the nodes
`for i in {0..3}; do python ./node.py -i $i -lf False  &; done`

## Send request to any one of the nodes
e.g. `curl -vLX POST --data '{ 'id': (0, 0), 'client_url': http://localhost:20001/reply,
    'timestamp': time.time(),'data': 'data_string'}' http://localhost:30000/request`
The `id` here is a tuple of `(client_id, seq_id)`, `client_url` is the url for sending request to the get_reply function,
`timestamp` is the current time, `data` is whatever data in string format, 

## Run the clients
`for i in {0...2}; do python client.py -id $i -nm 5 &; done`

## Environment
```
Python: 3.5.3
aiohttp: 3.4.4
yaml: 3.12
```
