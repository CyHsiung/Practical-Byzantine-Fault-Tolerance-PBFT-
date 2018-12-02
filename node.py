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


class MultiPaxosHandler:
    PREPARE = 'prepare'
    ACCEPT = 'accept'
    LEARN = 'learn'
    HEARTBEAT = 'heartbeat'
    REQUEST = 'request'
    SYNC = 'sync'

    NO_OP = 'NOP'

    def __init__(self, ind, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._me = self._nodes[ind]
        self._ind = ind

        ## leader election
        self._last_heartbeat = 0
        self._heartbeat_ttl = conf['heartbeat']['ttl']
        self._heartbeat_interval = conf['heartbeat']['interval']
        self._election_slice = conf['election_slice']

        ## leader
        # view_id = cur_time // election_slice; seq = self._seq
        # (view_id, seq)
        self._n = (0, 0)
        # Accumulated number of total proposals proposed by the given node
        self._seq = 0
        self._next_new_slot = 0
        self._is_leader = False
        self._hb_server = None

        ## acceptor
        # Indicate my current leader.
        self._leader = None
        ## better named n_promise_accept, the largest number either promised or accepted
        # Same format as self._n. Indicate current view and leader's sequence
        self._n_promise = (0, 0)
        self._accepted = {} # s: (n, v)

        ## learner
        self._sync_interval = conf['sync_interval']
        self._learning = {} # s: Counter(n: cnt)
        self._learned = [] # Bubble is represented by None
        self._learned_event = {} # s: event

        # Number of commit accumulated.
        self._accumulate_commit = 0
 
        # -
        self._loss_rate = conf['loss%'] / 100
        self._skip = conf['skip']

        # -
        self._network_timeout = conf['misc']['network_timeout']
        self._session = None
        self._log = logging.getLogger(__name__) 

        ## Update timeout for thew network system
        self._init_network_timeout = conf['misc']['network_timeout']
        self._init_heartbeat_interval = conf['heartbeat']['ttl']
        # Time record from send to ACK for last _numexec_time_records executions.
        self._network_time_queue = []
        self._num_exec_time_records = 100
        self._last_session_create_time = time.time()

    @staticmethod
    def make_url(node, command):
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

    async def _elect(self, view_id):
        '''
        Compete for election.

        1. Setup up `n`
        2. Call prepare(n, bubble_slots, next_new_slot) to all nodes
        3. Count vote
        4. New leader if possible
            1. Initial heartbeat
            2. Accept!
        '''
        self._log.info("---> %d: run election", self._ind)

        self._seq += 1
        self._n = (view_id, self._seq)

        # Only ask for things this proposer doesn't know.
        bubble_slots = []
        for i, v in enumerate(self._learned):
            if v == None:
                bubble_slots.append(i)

        json_data = {
                'leader': self._ind,
                'n': self._n,
                'bubble_slots': bubble_slots,
                'next_new_slot': len(self._learned),
                }

        resp_list = await self._make_requests(self._nodes, MultiPaxosHandler.PREPARE, json_data)

        # Count vote
        count = 0
        nv_for_known_slots = {}
        for i, resp in resp_list:
            if resp.status == 499: #TODO, give up
                self._log.warn("------> %d: give up on Nack", self._ind, i)
                return
            if resp.status == 200:
                try:
                    json_data = await resp.json()
                except:
                    pass
                else:
                    for s, (n, v) in json_data['known_slots'].items():
                        s = int(s)
                        if s in nv_for_known_slots:
                            nv_for_known_slots[s] = max(nv_for_known_slots[s], (n,v), key = lambda x: x[0])
                        else:
                            nv_for_known_slots[s] = (n, v)
                    count += 1

        self._log.info("------> %d: votes received %d", self._ind, count)

        # New leader now
        if count >= self._node_cnt // 2 + 1:
            self._log.info("------> %d: become new leader", self._ind)
            self._is_leader = True

            # Catch up
            proposal = {k: v[1] for k, v in nv_for_known_slots.items()}

            for b in bubble_slots: # fill in bubble
                if not b in proposal:
                    proposal[b] = MultiPaxosHandler.NO_OP

            if proposal:
                max_slot_in_proposal = max(proposal.keys())
            else:
                max_slot_in_proposal = -1
            self._next_new_slot = max(max_slot_in_proposal + 1, len(self._learned))
            for s in range(len(self._learned), self._next_new_slot):
                if s not in proposal:
                    proposal[s] = MultiPaxosHandler.NO_OP

            # First command: Accept!
            json_data = {
                    'leader': self._ind,
                    'n': self._n,
                    'proposal': proposal
                    }

            resp_list = await self._make_requests(self._nodes, MultiPaxosHandler.ACCEPT, json_data)
            for i, resp in resp_list:
                if resp.status == 499:
                    self._log.warn("------> %d: give up on Nack", self._ind, i)
                    # step down
                    self._is_leader = False
                    return

            # Initiate heartbeat!
            self._hb_server = asyncio.ensure_future(self._heartbeat_server())

    async def _heartbeat_server(self):
        '''
        Send heartbeat to every node.

        If get 499 as Nack, step down.
        '''
        while True:
            resp_list = await self._make_requests(self._nodes, MultiPaxosHandler.HEARTBEAT, { 'leader': self._ind, 'n': self._n })
            
            for i, resp in resp_list:
                if resp.status == 499:
                    # step down
                    self._is_leader = False
                    self._hb_server = None
                    return
            await asyncio.sleep(self._heartbeat_interval)

    async def heartbeat_observer(self):
        '''
        Observe hearbeat: if timeout, run election if appropriate.
        '''
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            cur_ts = time.time()
            if self._last_heartbeat < cur_ts - self._heartbeat_ttl: 
                self._log.warn("---> %d: heartbeat missing", self._ind)
                view_id = cur_ts // self._election_slice
                if view_id % self._node_cnt == self._ind:
                    # run election
                    asyncio.ensure_future(self._elect(view_id))

    async def heartbeat(self, request):
        '''
        Hearbeat sent from leader.
        {
            leader: 0,
            n: [ view_id, seq ],
        }

        respond with 200 if `n` is geq than current believed `n_promise`,
        else return 499
        '''

        json_data = await request.json()
        n_hb = tuple(json_data['n'])
        if n_hb > self._n_promise: #TODO: should be safe to update self._n_promise
            self._n_promise = n_hb
            self._leader = json_data['leader']
            self._last_heartbeat = time.time()
            code = 200
        elif n_hb == self._n_promise:
            self._last_heartbeat = time.time()
            code = 200
        else:
            code = 499

        return await self._make_response(web.Response(status=code))

    async def prepare(self, request):
        '''
        Prepare request from a proposer.

        {
            leader:
            n: 
            bubble_slots: []
            next_new_slot: 
        }

        respond with 200 as Ack if `n` is geq than current believed `n_promise`,
        else return 499 as Nack


        Ack:

        {
            known_slots: {
                0: (n, v),
                3: (n, v)
            }
        }
        '''
        self._log.info("---> %d: on prepare", self._ind)

        json_data = await request.json()
        n_prepare = tuple(json_data['n'])
        if n_prepare > self._n_promise: # Only Ack if n_prepare larger than the largest n ever seen.
            self._n_promise = n_prepare
            self._leader = json_data['leader']
            self._last_heartbeat = time.time() # also treat as a heartbeat

            bubble_slots = json_data['bubble_slots']
            next_new_slot = json_data['next_new_slot']

            known_slots = {}
            for b in bubble_slots:
                if b in self._accepted:
                    known_slots[b] = self._accepted[b]
                elif b < len(self._learned) and self._learned[b]: # return learned value directly, fake `n`
                    known_slots[b] = (n_prepare, self._learned[b])

            for b in self._accepted:
                if b >= next_new_slot:
                    known_slots[b] = self._accepted[b]

            for i in range(next_new_slot, len(self._learned)):
                if self._learned[i]:
                    known_slots[i] = (n_prepare, self._learned[i])

            json_data = {'known_slots': known_slots}
            ret = web.json_response(json_data)
        else:
            ret = web.Response(status=499)

        return await self._make_response(ret)

    async def accept(self, request):
        '''
        Accept request from a proposer.

        Accept: {
            leader:
            n: 
            proposal: {
                0: v,
                13: v,
            }
        }

        respond with 200 as OK if `n` is geq than current believed `n_promise`,
        else return 499 as Nack

        Make LEARN request to all nodes if accepted.
        '''
        self._log.info("---> %d: on accept", self._ind)

        json_data = await request.json()
        n_accept = tuple(json_data['n'])
        if n_accept >= self._n_promise: # Only Accept if n_accept geq than the largest n ever seen.
            self._n_promise = n_accept
            self._leader = json_data['leader']
            proposal = json_data['proposal']
            for s, v in proposal.items():
                s = int(s)
                if not s < len(self._learned) or not self._learned[s]:
                    self._accepted[s] = (n_accept, v)
                asyncio.ensure_future(self._make_requests(self._nodes, MultiPaxosHandler.LEARN, { 'n': n_accept, 'proposal': proposal }))
            ret = web.Response()
        else:
            ret = web.Response(status=499)

        return await self._make_response(ret)

    async def learn(self, request):
        '''
        Learn request from nodes.

        {
            n: 
            proposal: {
                0: v,
                3: v,
            }
        }

        No response needed
        '''
        self._log.info("---> %d: on learn", self._ind)

        json_data = await request.json()
        proposal = json_data['proposal']
        n = tuple(json_data['n'])
        for s, v in proposal.items():
            s = int(s)
            if not s < len(self._learned) or not self._learned[s]:
                if not s in self._learning:
                    self._learning[s] = Counter()
                self._learning[s][n] += 1
                if self._learning[s][n] >= self._node_cnt // 2 + 1:
                    if not s < len(self._learned):
                        self._learned += [None] * (s + 1 - len(self._learned))
                    self._learned[s] = v
                    # Delete useless info after learn the given slot.
                    if s in self._learning:
                        del self._learning[s]
                    if s in self._accepted:
                        del self._accepted[s]
            
                    while self._accumulate_commit < len(self._learned) and self._learned[self._accumulate_commit] != None:
                        if self._accumulate_commit in self._learned_event:
                            self._learned_event[self._accumulate_commit].set()
                            del self._learned_event[self._accumulate_commit]
                        self._accumulate_commit += 1
        return await self._make_response(web.Response())

    async def decision_synchronizer(self):
        '''
        Sync with other learner periodically.

        Input/output similar to `prepare`
    
        {
            known_slots: {
                0: v,
                3: v
            }
        }
        '''
        while True:
            await asyncio.sleep(self._sync_interval)
            self._log.info("%d:  Start synchronizing.", self._ind)
            if not self._is_leader and self._leader != None:
                # Only ask for things this learner doesn't know.
                bubble_slots = []
                for i, v in enumerate(self._learned):
                    if v == None:
                        self._log.info("%d: with bubble at slot %d.", self._ind, i)
                        bubble_slots.append(i)

                json_data = {
                        'bubble_slots': bubble_slots,
                        'next_new_slot': len(self._learned),
                        }

                # Leader nodehas the most updated information. As a result,
                # only need to update learned result with leader node.
                self._log.debug("%d:  Start asking filling the bubble.", self._ind)
                
                resp_list = await self._make_requests([self._nodes[self._leader]], MultiPaxosHandler.SYNC, json_data)
                self._log.debug("%d:  Receive bubble filling message.", self._ind)
                if resp_list:
                    resp = resp_list[0][1]
                    try:
                        json_data = await resp.json()
                    except:
                        self._log.info("%d: Bad known slot message.", self._ind)
                        pass
                    else:
                        for s, v in json_data['known_slots'].items():
                            s = int(s)
                            self._log.debug("%d: Update bubble %d by decision synchrosizer.", self._ind, s)
                            if not s < len(self._learned):
                                self._learned += [None] * (s + 1 - len(self._learned))
                            self._learned[s] = v
                            # Delete useless info after learn the given slot.
                            if s in self._learning:
                                del self._learning[s]
                            if s in self._accepted:
                                del self._accepted[s]
                            # Once the former are all executed(Without bubble), we send ACK to the client of given slot. 
                            while self._accumulate_commit < len(self._learned) and self._learned[self._accumulate_commit] != None:
                                if self._accumulate_commit in self._learned_event:
                                    self._learned_event[self._accumulate_commit].set()
                                    del self._learned_event[self._accumulate_commit]
                                self._accumulate_commit += 1
            self._log.info("%d: accumulate commit: %d" , self._ind, self._accumulate_commit)
            
            with open("{}.dump".format(self._ind), 'w') as f:
                json.dump(self._learned[:self._accumulate_commit], f)

    async def sync(self, request):
        '''
        Sync request from learner.

        {
            bubble_slots: []
            next_new_slot: 
        }
        '''
        self._log.info("---> %d: on sync", self._ind)

        json_data = await request.json()

        bubble_slots = json_data['bubble_slots']
        next_new_slot = json_data['next_new_slot']

        known_slots = {}
        for b in bubble_slots:
            if b < len(self._learned) and self._learned[b]:
                known_slots[str(b)] = self._learned[b]
                self._log.info("%d: Fill bubble %d by sync.", self._ind, b)
        for i in range(next_new_slot, len(self._learned)):
            if self._learned[i]:
                known_slots[str(i)] = self._learned[i]

        json_data = {'known_slots': known_slots}
        
        return await self._make_response(web.json_response(json_data))

    async def request(self, request):
        '''
        Request from client.

        {
            id: (cid, seq),
            data: "string"
        }

        Handle this if leader, otherwise redirect to leader
        '''
        self._log.info("---> %d: on request", self._ind)

        json_data = await request.json()
        if not self._is_leader:
            if self._leader != None:
                raise web.HTTPTemporaryRedirect(self.make_url(self._nodes[self._leader], MultiPaxosHandler.REQUEST))
            else:
                raise web.HTTPServiceUnavailable()

        this_slot = self._next_new_slot
        if this_slot == self._skip:
            this_slot += 1
        self._next_new_slot = this_slot + 1
        proposal = {
                'leader': self._ind,
                'n': self._n,
                'proposal': {
                    this_slot: [json_data['id'], json_data['data']]
                    }
                }
        if not this_slot in self._learned_event:
            self._learned_event[this_slot] = asyncio.Event()
        this_event = self._learned_event[this_slot]

        resp_list = await self._make_requests(self._nodes, MultiPaxosHandler.ACCEPT, proposal)
        for i, resp in resp_list:
            if resp.status == 499:
                self._log.warn("------> %d: give up on Nack from %d", self._ind, i)
                # step down
                self._is_leader = False
                if self._hb_server:
                    self._hb_server.cancel()
                    self._hb_server = None
                if self._leader != None and self._leader != self._ind:
                    redirect = self._leader
                else:
                    redirect = i
                raise web.HTTPTemporaryRedirect(self.make_url(self._nodes[redirect], MultiPaxosHandler.REQUEST))

        # respond only when learner learned the value
        await this_event.wait()
        resp = web.Response()
        
        if self._learned[this_slot] == [json_data['id'], json_data['data']]:
            resp = web.Response()
        else:
            if self._leader != None and self._leader != self._ind:
                redirect = self._leader
            else:
                redirect = randint(0, self._node_cnt-1)
            raise web.HTTPTemporaryRedirect(self.make_url(self._nodes[redirect], MultiPaxosHandler.REQUEST))
        
        return await self._make_response(resp)


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
    asyncio.ensure_future(paxos.heartbeat_observer())
    asyncio.ensure_future(paxos.decision_synchronizer())

    app = web.Application()
    app.add_routes([
        web.post('/' + MultiPaxosHandler.HEARTBEAT, paxos.heartbeat),
        web.post('/' + MultiPaxosHandler.PREPARE, paxos.prepare),
        web.post('/' + MultiPaxosHandler.ACCEPT, paxos.accept),
        web.post('/' + MultiPaxosHandler.LEARN, paxos.learn),
        web.post('/' + MultiPaxosHandler.SYNC, paxos.sync),
        web.post('/' + MultiPaxosHandler.REQUEST, paxos.request),
        ])

    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    main()

