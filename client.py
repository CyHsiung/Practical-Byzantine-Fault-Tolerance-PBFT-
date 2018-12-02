#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
import json
import asyncio
import aiohttp
from aiohttp import web
from random import random



def logging_config(log_level=logging.INFO, log_file=None):
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return

    root_logger.setLevel(log_level)

    f = logging.Formatter("[%(levelname)s]%(module)s->%(funcName)s: \t %(message)s \t --- %(asctime)s")

    h = logging.StreamHandler()
    h.setFormatter(f)
    h.setLevel(log_level)
    root_logger.addHandler(h)

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        h = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        h.setFormatter(f)
        h.setLevel(log_level)
        root_logger.addHandler(h)

def arg_parse():
    # parse command line options
    parser = argparse.ArgumentParser(description='Multi-Paxos Node')
    parser.add_argument('-id', '--client_id', type=int, help='client id')
    parser.add_argument('-nm', '--num_messages', default=10, type=int, help='number of message want to send for this client')
    parser.add_argument('-c', '--config', default='paxos.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    '''
    Sample config:

    nodes:
        - host: 
          port:
        - host:
          port:

    loss%:

    skip:

    heartbeat:
        ttl:
        interval:

    election_slice: 10

    sync_interval: 10

    misc:
        network_timeout: 10
    '''
    conf = yaml.load(conf_file)
    return conf

def make_url(node, command):
    return "http://{}:{}/{}".format(node['host'], node['port'], command)

async def SendData(conf, args, log):
    nodes = conf['nodes']
    resend_interval = conf['misc']['network_timeout']
    is_sent = False
    client_id = args.client_id
    num_messages = args.num_messages

    timeout = aiohttp.ClientTimeout(resend_interval)
    session = aiohttp.ClientSession(timeout = timeout)
 
    for i in range(num_messages):
        is_sent = False
        json_data = {
                'id': (client_id, i),
                'data': "client_id = " + str(client_id) + " # of message: " + str(i),
                }
        dest_ind = 0
        # Every time succeed in sending message, wait for 0 - 1 second.
        await asyncio.sleep(random())
        while 1:
            try:
                await asyncio.wait_for(session.post(make_url(nodes[dest_ind], 'request'), 
                    json=json_data), resend_interval)
            except:
                log.info("---> %d message %d sent fail.", client_id, i)
                pass
            else:
                log.info("---> %d message %d sent successfully..", client_id, i)
                is_sent = True
            if is_sent:
                break
    session.close()
    

def main():
    logging_config()
    log = logging.getLogger()
    args = arg_parse()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['nodes'][0]
    log.info("begin")
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(SendData(conf, args, log))

if __name__ == "__main__":
    main()

            
    
