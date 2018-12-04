#! /usr/bin/env python3
import logging
import argparse
import yaml
import time
from random import random, randint
from collections import Counter
import json
import sys

import asyncio
import aiohttp
from aiohttp import web

import hashlib

class View:
    def __init__(self, leader_node = 0, times_for_leader = 0):
        self.leader_node = leader_node
        self.times_for_leader = times_for_leader
    # To encode to json
    def _to_tuple(self):
        return (self.leader_node, self.times_for_leader)
    # Recover from json data.
    def _update_from_tuple(self, view_tuple):
        self.leader_node = view_tuple[0]
        self.times_for_leader = view_tuple[1]

class Status:
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = "reply"

    def __init__(self, f):
        self.f = f
        self.request = 0
        self.prepare_msgs = {}     
        self.prepare_certificate = None # proposal
        self.commit_msgs = {} 
        self.commit_certificate = None # proposal
        

    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, msg_type, view, proposal, from_node):
        '''
        Update the record in the status by message type
        input:
            msg_type: self.PREPARE or self.COMMIT
            view: View object of self._follow_view
            proposal: proposal in json_data
            from_node: The node send given the message.
        '''

        # The key need to include hash(proposal) in case get different 
        # preposals from BFT nodes. Need sort key in json.dumps to make 
        # sure getting the same string. Use hashlib so that we got same 
        # hash everytime.
        hash_object = hashlib.md5(json.dumps(proposal, sort_keys=True).encode())
        key = (view._to_tuple(), hash_object.digest())
        if msg_type == Status.PREPARE:
            if key not in self.prepare_msgs:
                self.prepare_msgs[key] = self.SequenceElement(proposal)
            self.prepare_msgs[key].from_nodes.add(from_node)
        elif msg_type == Status.COMMIT:
            if key not in self.commit_msgs:
                self.commit_msgs[key] = self.SequenceElement(proposal)
            self.commit_msgs[key].from_nodes.add(from_node)

    def _check_majority(self, msg_type):
        '''
        Check if receive more than 2f + 1 given type message in the same view.
        input:
            msg_type: self.PREPARE or self.COMMIT
        '''
        if msg_type == Status.PREPARE:
            if self.prepare_certificate:
                return True
            for key in self.prepare_msgs:
                if len(self.prepare_msgs[key].from_nodes)>= 2 * self.f + 1:
                    return True
            return False

        if msg_type == Status.COMMIT:
            if self.commit_certificate:
                return True
            for key in self.commit_msgs:
                if len(self.commit_msgs[key].from_nodes) >= 2 * self.f + 1:
                    return True
            return False 




class MultiPaxosHandler:
    REQUEST = 'request'
    PREPREPARE = 'preprepare'
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = 'reply'
    HEARTBEAT = 'heartbeat'
    
    SYNC = 'sync'

    NO_OP = 'NOP'

    def __init__(self, index, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._me = self._nodes[index]
        self._index = index
        # Number of faults tolerant.
        self.f = (self._node_cnt - 1) // 3

        # leader election
        self._last_heartbeat = 0
        self._heartbeat_ttl = conf['heartbeat']['ttl']
        self._heartbeat_interval = conf['heartbeat']['interval']
        self._election_slice = conf['election_slice']

        # leader
        self._view = View(self._index, 0)
        self._next_new_slot = 0

        # TODO: Test fixed
        if self._index == 0:
            self._is_leader = True
        else:
            False

        self._hb_server = None

        # Indicate my current leader.

        # TODO: Test fixed
        self._leader = 0
        # The largest view either promised or accepted
        # Same format as View._to_tuple
        self._follow_view = View(0, 0)
        
        # Record all the status of the given slot
        self._status_by_slot = {}

        # learner
        self._sync_interval = conf['sync_interval']
        self._learning = {} # s: Counter(n: cnt)
        self._learned = [] # Bubble is represented by None
        self._learned_event = {} # s: event
 
        # -
        self._loss_rate = conf['loss%'] / 100

        # -
        self._network_timeout = conf['misc']['network_timeout']
        self._session = None
        self._log = logging.getLogger(__name__) 

        ## Update timeout for thew network system
        self._heartbeat_interval = conf['heartbeat']['ttl']

    @staticmethod
    def make_url(node, command):
        '''
        input: 
            node: dictionary with key of host(url) and port
            command: action
        output:
            The url to send with given node and action.
        '''
        return "http://{}:{}/{}".format(node['host'], node['port'], command)

    async def _make_requests(self, nodes, command, json_data):
        '''
        Send json data:

        input:
            nodes: list of dictionary with key: host, port
            command: Command to execute.
            json_data: Json data.
        output:
            list of tuple: (node_index, response)

        '''
        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                if not self._session:
                    timeout = aiohttp.ClientTimeout(self._network_timeout)
                    self._session = aiohttp.ClientSession(timeout=timeout)
                self._log.debug("make request to %d, %s", i, command)
                try:
                    resp = await self._session.post(self.make_url(node, command), json=json_data)
                    resp_list.append((i, resp))
                    
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass
        return resp_list 

    async def _make_response(self, resp):
        '''
        Drop response by chance, via sleep for sometime.
        '''
        if random() < self._loss_rate:
            await asyncio.sleep(self._network_timeout)
        return resp

    async def _post(self, nodes, command, json_data):
        '''
        Broadcast json_data to all node in nodes with given command.
        input:
            nodes: list of nodes
            command: action
            json_data: Data in json format.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("make request to %d, %s", i, command)
                try:
                    _ = await self._session.post(self.make_url(node, command), json=json_data)
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass

    async def preprepare(self, request):
        '''
        Prepare: Deal with request from the client and broadcast to other replicas.
        input:
            request: web request from client
                json = 
                {
                    id: (cid, seq),
                    client_url: "url string"
                    timestamp:"time"
                    data: "string"
                }

        '''

        json_data = await request.json()

        self._log.info("---> %d: on preprepare", self._index)
        this_slot = self._next_new_slot
        self._next_new_slot = this_slot + 1

        if this_slot not in self._status_by_slot:
            self._status_by_slot[this_slot] = Status(self.f)
        self._status_by_slot[this_slot].request = json_data

        preprepare_msg = {
            'leader': self._index,
            'view': self._view._to_tuple(),
            'proposal': {
                this_slot: json_data
            },
            'type': 'preprepare'
        }
        
        await self._post(self._nodes, MultiPaxosHandler.PREPARE, preprepare_msg)



    async def get_request(self, request):
        '''
        Handle the request from client if leader, otherwise 
        redirect to the leader.
        '''
        self._log.info("---> %d: on request", self._index)

        if not self._is_leader:
            if self._leader != None:
                raise web.HTTPTemporaryRedirect(self.make_url(self._nodes[self._leader], MultiPaxosHandler.REQUEST))
            else:
                raise web.HTTPServiceUnavailable()
        else:
            await self.preprepare(request)
            return web.Response()

    async def prepare(self, request):
        '''
        Once receive preprepare message from client, broadcast 
        prepare message to all replicas.

        input: 
            request: preprepare message from preprepare:
                preprepare_msg = {
                    'leader': self._index,
                    'view': self._view._to_tuple,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'preprepare'
                }

        '''
        
        json_data = await request.json()

        if tuple(json_data['view']) < self._follow_view._to_tuple():
            # when receive message with view < follow_view, do nothing
            return web.Response()
        elif tuple(json_data['view']) > self._follow_view._to_tuple():

            # when receive message with view > follow_view, update view
            self._follow_view._update_from_tuple(tuple(json_data['view']))

        self._log.info("---> %d: receive preprepare msg from %d", self._index, json_data['leader'])
        self._log.info("---> %d: on prepare", self._index)
        for slot in json_data['proposal']:
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self.f)

            prepare_msg = {
                'index': self._index,
                'view': json_data['view'],
                'proposal': {
                    slot: json_data['proposal'][slot]
                },
                'type': Status.PREPARE
            }
            await self._post(self._nodes, MultiPaxosHandler.COMMIT, prepare_msg)
        return web.Response()

    async def commit(self, request):
        '''
        Once receive more than 2f + 1 prepare message,
        send the prepare message
        input:
            request: prepare message from preprepare:
                prepare_msg = {
                    'index': self._index,
                    'view': self._n,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'prepare'
                }
        '''
        json_data = await request.json()
        self._log.info("---> %d: receive prepare msg from %d", self._index, json_data['index'])


        # when receive message with view < follow_view, do nothing
        if tuple(json_data['view']) < self._follow_view._to_tuple():
            return web.Response()

        self._log.info("---> %d: on commit", self._index)
        
        for slot in json_data['proposal']:
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self.f)
            status = self._status_by_slot[slot]

            view = View()
            view._update_from_tuple(tuple(json_data['view']))

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            if status._check_majority(json_data['type']):
                status.prepare_certificate = json_data['proposal'][slot]
                commit_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': {
                        slot: json_data['proposal'][slot]
                    },
                    'type': Status.COMMIT
                }
                await self._post(self._nodes, MultiPaxosHandler.REPLY, commit_msg)
        return web.Response()

    async def reply(self, request):
        '''
        Once receive more than 2f + 1 commit message,
        reply to the client.
        input:
            request: commit message from prepare:
                preprepare_msg = {
                    'index': self._index,
                    'n': self._n,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'commit'
                }
        '''
        
        json_data = await request.json()
        self._log.info("---> %d: on reply", self._index)

        # when receive message with view < follow_view, do nothing
        if tuple(json_data['view']) < self._follow_view._to_tuple():
            return web.Response()

        self._log.info("---> %d: receive commit msg from %d", self._index, json_data['index'])

        for slot in json_data['proposal']:
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self.f)
            status = self._status_by_slot[slot]

            view = View()
            view._update_from_tuple(tuple(json_data['view']))

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            # Reply only once.
            if status._check_majority(json_data['type']) and not status.commit_certificate:
                status.commit_certificate = json_data['proposal'][slot]

                reply_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': json_data['proposal'][slot],
                    'type': Status.REPLY
                }

                self._log.info("%d reply to %s successfully!!", 
                    self._index, json_data['proposal'][slot]['client_url'])
                
                try:
                    await self._session.post(json_data['proposal'][slot]['client_url'], json=reply_msg)
                except:
                    pass
                
        return web.Response()

    async def sync(self):
        pass

    async def heartbeat(self):
        pass

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
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='paxos.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
    parser.add_argument('-lf', '--log_to_file', default=False, type=bool, help='Whether to dump log messages to file, default = False')
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

def main():
    args = arg_parse()
    if args.log_to_file:
        logging.basicConfig(filename='log_' + str(args.index),
                            filemode='a', level=logging.DEBUG)
    logging_config()
    log = logging.getLogger()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['nodes'][args.index]
    host = addr['host']
    port = addr['port']

    paxos = MultiPaxosHandler(args.index, conf)
    # asyncio.ensure_future(paxos.heartbeat_observer())
    # asyncio.ensure_future(paxos.decision_synchronizer())

    app = web.Application()
    app.add_routes([
        web.post('/' + MultiPaxosHandler.REQUEST, paxos.get_request),
        web.post('/' + MultiPaxosHandler.PREPREPARE, paxos.preprepare),
        web.post('/' + MultiPaxosHandler.PREPARE, paxos.prepare),
        web.post('/' + MultiPaxosHandler.COMMIT, paxos.commit),
        web.post('/' + MultiPaxosHandler.REPLY, paxos.reply),
        web.post('/' + MultiPaxosHandler.SYNC, paxos.sync),
        web.post('/' + MultiPaxosHandler.HEARTBEAT, paxos.heartbeat),
        ])

    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    main()

