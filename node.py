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
    def __init__(self, view_number = 0, times_for_leader = 0):
        self.view_number = view_number
        self.times_for_leader = times_for_leader
    # To encode to json
    def _to_tuple(self):
        return (self.view_number, self.times_for_leader)
    # Recover from json data.
    def _update_from_tuple(self, view_tuple):
        self.view_number = view_tuple[0]
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
        # Only means receive more than 2f + 1 commit message,
        # but can not commit if there are any bubbles previously.
        self.commit_certificate = None # proposal

        # Set it to True only after commit
        self.is_committed = False
    
    class Certificate:
        def __init__(self, view = 0, proposal = 0):
            '''
            input:
                view: object of class View
                proposal: proposal in json_data(dict)
            '''
            self._view = view
            self._proposal = proposal

        def to_dict(self):
            '''
            Convert the Certificate to dictionary
            '''
            return {
                'view': self._view._to_tuple(),
                'proposal': self._proposal
            }

        def dumps_from_dict(self, dictionary):
            '''
            Update the view from the form after self.to_dict
            input:
                dictionay = {
                    'view': self._view._to_tuple(),
                    'proposal': self._proposal
                }
            '''
            view = View()
            # print(dictionary)
            view._update_from_tuple(dictionary['view'])
            self._view = view
            self._proposal = dictionary['proposal']


    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, msg_type, view, proposal, from_node):
        '''
        Update the record in the status by message type
        input:
            msg_type: Status.PREPARE or Status.COMMIT
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

class CheckPoint:
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'
    def __init__(self, checkpoint_interval, nodes, f, node_index, 
            lose_rate = 0, network_timeout = 10):
        self._checkpoint_interval = checkpoint_interval
        self._nodes = nodes
        self._f = f
        self._node_index = node_index
        self._loss_rate = lose_rate
        self._log = logging.getLogger(__name__) 
        # Next slot of the given globally accepted checkpoint.
        # For example, the current checkpoint record until slot 99
        # next_slot = 100
        self.next_slot = 0
        # Globally accepted checkpoint
        self.checkpoint = []
        # Use the hash of the checkpoint to record receive vates for given ckpt.
        self._received_votes_by_ckpt = {} 
        self._session = None
        self._network_timeout = network_timeout

        self._log.info("---> %d: Create checkpoint.", self._node_index)

    # Class to record the status of received checkpoints
    class ReceiveVotes:
        def __init__(self, ckpt, next_slot):
            self.from_nodes = set([])
            self.checkpoint = ckpt
            self.next_slot = next_slot

    def get_commit_upperbound(self):
        '''
        Return the upperbound that could commit 
        (return upperbound = true upperbound + 1)
        '''
        return self.next_slot + 2 * self._checkpoint_interval

    def _hash_ckpt(self, ckpt):
        '''
        input: 
            ckpt: the checkpoint
        output:
            The hash of the input checkpoint in the format of 
            binary string.
        '''
        hash_object = hashlib.md5(json.dumps(ckpt, sort_keys=True).encode())
        return hash_object.digest()  


    async def receive_vote(self, ckpt_vote):
        '''
        Trigger when PBFTHandler receive checkpoint votes.
        First, we update the checkpoint status. Second, 
        update the checkpoint if more than 2f + 1 node 
        agree with the given checkpoint.
        input: 
            ckpt_vote = {
                'node_index': self._node_index
                'next_slot': self._next_slot + self._checkpoint_interval
                'ckpt': json.dumps(ckpt)
                'type': 'vote'
            }
        '''
        self._log.debug("---> %d: Receive checkpoint votes", self._node_index)
        ckpt = json.loads(ckpt_vote['ckpt'])
        next_slot = ckpt_vote['next_slot']
        from_node = ckpt_vote['node_index']

        hash_ckpt = self._hash_ckpt(ckpt)
        if hash_ckpt not in self._received_votes_by_ckpt:
            self._received_votes_by_ckpt[hash_ckpt] = (
                CheckPoint.ReceiveVotes(ckpt, next_slot))
        status = self._received_votes_by_ckpt[hash_ckpt]
        status.from_nodes.add(from_node)
        for hash_ckpt in self._received_votes_by_ckpt:
            if (self._received_votes_by_ckpt[hash_ckpt].next_slot > self.next_slot and 
                    len(self._received_votes_by_ckpt[hash_ckpt].from_nodes) >= 2 * self._f + 1):
                self._log.info("---> %d: Update checkpoint by receiving votes", self._node_index)
                self.next_slot = self._received_votes_by_ckpt[hash_ckpt].next_slot
                self.checkpoint = self._received_votes_by_ckpt[hash_ckpt].checkpoint


    async def propose_vote(self, commit_decisions):
        '''
        When node the slots of committed message exceed self._next_slot 
        plus self._checkpoint_interval, propose new checkpoint and 
        broadcast to every node

        input: 
            commit_decisions: list of tuple: [((client_index, client_seq), data), ... ]

        output:
            next_slot for the new update and garbage collection of the Status object.
        '''
        proposed_checkpoint = self.checkpoint + commit_decisions
        await self._broadcast_checkpoint(proposed_checkpoint, 
            'vote', CheckPoint.RECEIVE_CKPT_VOTE)


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
                    _ = await self._session.post(
                        self.make_url(node, command), json=json_data)
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass

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


    async def _broadcast_checkpoint(self, ckpt, msg_type, command):
        json_data = {
            'node_index': self._node_index,
            'next_slot': self.next_slot + self._checkpoint_interval,
            'ckpt': json.dumps(ckpt),
            'type': msg_type
        }
        await self._post(self._nodes, command, json_data)

    def get_ckpt_info(self):

        '''
        Get the checkpoint serialized information.Called 
        by synchronize function to get the checkpoint
        information.
        '''
        json_data = {
            'next_slot': self.next_slot,
            'ckpt': json.dumps(self.checkpoint)
        }
        return json_data

    def update_checkpoint(self, json_data):
        '''
        Update the checkpoint when input checkpoint cover 
        more slots than current.
        input: 
            json_data = {
                'next_slot': self._next_slot
                'ckpt': json.dumps(ckpt)
            }     
        '''
        self._log.debug("update_checkpoint: next_slot: %d; update_slot: %d"
            , self.next_slot, json_data['next_slot'])
        if json_data['next_slot'] > self.next_slot:
            self._log.info("---> %d: Update checkpoint by synchronization.", self._node_index)
            self.next_slot = json_data['next_slot']
            self.checkpoint = json.loads(json_data['ckpt'])
        

    async def receive_sync(sync_ckpt):
        '''
        Trigger when recieve checkpoint synchronization messages.
        input: 
            sync_ckpt = {
                'node_index': self._node_index
                'next_slot': self._next_slot + self._checkpoint_interval
                'ckpt': json.dumps(ckpt)
                'type': 'sync'
            }
        '''
        self._log.debug("receive_sync: next_slot: %d; update_slot: %d"
            , self.next_slot, json_data['next_slot'])
        if sync_ckpt['next_slot'] > self._next_slot:
            self.next_slot = sync_ckpt['next_slot']
            self.checkpoint = json.loads(sync_ckpt['ckpt'])

    async def garbage_collection(self):
        '''
        Clean those ReceiveCKPT objects whose next_slot smaller
        than or equal to the current.
        '''
        deletes = []
        for hash_ckpt in self._received_votes_by_ckpt:
            if self._received_votes_by_ckpt[hash_ckpt].next_slot <= next_slot:
                deletes.append(hash_ckpt)
        for hash_ckpt in deletes:
            del self._received_votes_by_ckpt[hash_ckpt] 

class PBFTHandler:
    REQUEST = 'request'
    PREPREPARE = 'preprepare'
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = 'reply'

    NO_OP = 'NOP'

    RECEIVE_SYNC = 'receive_sync'
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'

    def __init__(self, index, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._me = self._nodes[index]
        self._index = index
        # Number of faults tolerant.
        self._f = (self._node_cnt - 1) // 3

        # leader
        self._view = View(self._index, 0)
        self._next_propose_slot = 0

        # TODO: Test fixed
        if self._index == 0:
            self._is_leader = True
        else:
            self._is_leader = False

        # Network simulation
        self._loss_rate = conf['loss%'] / 100

        # Time configuration
        self._network_timeout = conf['misc']['network_timeout']

        # Checkpoint
        self._checkpoint_interval = conf['ckpt_interval']
        self._ckpt = CheckPoint(self._checkpoint_interval, self._nodes, 
            self._f, self._index, self._loss_rate, self._network_timeout)
        # Commit
        self._last_commit_slot = -1

        # Indicate my current leader.
        # TODO: Test fixed
        self._leader = 0

        # The largest view either promised or accepted
        # Same format as View._to_tuple
        self._follow_view = View(0, 0)
        
        # Record all the status of the given slot
        # To adjust json key, slot is string integer.
        self._status_by_slot = {}

        self._sync_interval = conf['sync_interval']
 
        
        self._session = None
        self._log = logging.getLogger(__name__) 


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

    def _legal_slot(self, slot):
        '''
        the slot is legal only when it's between upperbound and the lowerbound.
        input:
            slot: string integer direct get from the json_data proposal key.
        output:
            boolean to express the result.
        '''
        if int(slot) < self._ckpt.next_slot or int(slot) >= self._ckpt.get_commit_upperbound():
            return False
        else:
            return True

    async def preprepare(self, request):
        '''
        Prepare: Deal with request from the client and broadcast to other replicas.
        input:
            request: web request from client
                json = 
                {
                    id: (client_id, client_seq),
                    client_url: "url string"
                    timestamp:"time"
                    data: "string"
                }

        '''

        json_data = await request.json()

        self._log.info("---> %d: on preprepare", self._index)
        this_slot = str(self._next_propose_slot)
        self._next_propose_slot = int(this_slot) + 1

        if this_slot not in self._status_by_slot:
            self._status_by_slot[this_slot] = Status(self._f)
        self._status_by_slot[this_slot].request = json_data

        preprepare_msg = {
            'leader': self._index,
            'view': self._view._to_tuple(),
            'proposal': {
                this_slot: json_data
            },
            'type': 'preprepare'
        }
        
        await self._post(self._nodes, PBFTHandler.PREPARE, preprepare_msg)



    async def get_request(self, request):
        '''
        Handle the request from client if leader, otherwise 
        redirect to the leader.
        '''
        self._log.info("---> %d: on request", self._index)

        if not self._is_leader:
            if self._leader != None:
                raise web.HTTPTemporaryRedirect(self.make_url(self._nodes[self._leader], PBFTHandler.REQUEST))
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

            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)

            prepare_msg = {
                'index': self._index,
                'view': json_data['view'],
                'proposal': {
                    slot: json_data['proposal'][slot]
                },
                'type': Status.PREPARE
            }
            await self._post(self._nodes, PBFTHandler.COMMIT, prepare_msg)
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
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View()
            view._update_from_tuple(tuple(json_data['view']))

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            if status._check_majority(json_data['type']):
                status.prepare_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])
                commit_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': {
                        slot: json_data['proposal'][slot]
                    },
                    'type': Status.COMMIT
                }
                await self._post(self._nodes, PBFTHandler.REPLY, commit_msg)
        return web.Response()

    async def reply(self, request):
        '''
        Once receive more than 2f + 1 commit message,
        append the commit certificate and lock.
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
            if not self._legal_slot(slot):
                continue
            
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View()
            view._update_from_tuple(tuple(json_data['view']))

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            # Commit only when no commit certificate and got more than 2f + 1
            # commit message.
            if not status.commit_certificate and status._check_majority(json_data['type']):
                status.commit_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])
                
                # Reply only once and only when no bubble ahead
                if self._last_commit_slot == int(slot) - 1 and not status.is_committed:

                    reply_msg = {
                        'index': self._index,
                        'view': json_data['view'],
                        'proposal': json_data['proposal'][slot],
                        'type': Status.REPLY
                    }
                    status.is_committed = True
                    self._last_commit_slot += 1

                    # When commit messages fill the next checkpoint, 
                    # propose a new checkpoint.
                    if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                        await self._ckpt.propose_vote(self.get_commit_decisions())
                        self._log.info("---> %d: Propose checkpoint with last slot: %d. "
                            "In addition, current checkpoint's next_slot is: %d", 
                            self._index, self._last_commit_slot, self._ckpt.next_slot)


                    # Commit!
                    await self._commit_action()
                    try:
                        await self._session.post(json_data['proposal'][slot]['client_url'], json=reply_msg)
                    except:
                        self._log.error("Send message failed to %s", json_data['proposal'][slot]['client_url'])
                        pass
                    else:
                        self._log.info("%d reply to %s successfully!!", 
                            self._index, json_data['proposal'][slot]['client_url'])
                
        return web.Response()

    def get_commit_decisions(self):
        '''
        Get the commit decision between the next slot of the 
        current ckpt until last commit slot
        output:
            commit_decisions: list of tuple: [((client_index, client_seq), data), ... ]

        '''
        commit_decisions = []
        for i in range(self._ckpt.next_slot, self._last_commit_slot + 1):
            status = self._status_by_slot[str(i)]
            proposal = status.commit_certificate._proposal
            commit_decisions.append((proposal['id'], proposal['data']))

        return commit_decisions

    async def _commit_action(self):
        '''
        Dump the current commit decisions to disk.
        '''
        with open("{}.dump".format(self._index), 'w') as f:
            dump_data = self._ckpt.checkpoint + self.get_commit_decisions()
            json.dump(dump_data, f)

    async def receive_ckpt_vote(self, request):
        '''
        Receive the message sent from CheckPoint.propose_vote()
        '''
        self._log.debug("---> %d: receive checkpoint vote.", self._index)
        json_data = await request.json()
        await self._ckpt.receive_vote(json_data)
        return web.Response()

    async def receive_sync(self, request):
        '''
        Update the checkpoint and fill the bubble when receive sync messages.
        input:
            json_data = {
                'checkpoint': json_data = {
                    'next_slot': self._next_slot
                    'ckpt': json.dumps(ckpt)
                }
                'commit_certificates':commit_certificates
                    (Elements are commit_certificate.to_dict())
            }
        '''
        self._log.info("---> %d: on receive sync stage.", self._index)
        json_data = await request.json()
        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
        # TODO: Only check bubble instead of all slots between lowerbound
        # and upperbound of the commit.

        for slot in json_data['commit_certificates']:
            # Skip those slot not qualified for update.
            if int(slot) >= self._ckpt.get_commit_upperbound() or (
                    int(slot) < self._ckpt.next_slot):
                continue

            certificate = json_data['commit_certificates'][slot]
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
                commit_certificate = Status.Certificate()
                commit_certificate.dumps_from_dict(certificate)
                self._status_by_slot[slot].commit_certificate =  commit_certificate
            elif not self._status_by_slot[slot].commit_certificate:
                commit_certificate = Status.Certificate()
                commit_certificate.dumps_from_dict(certificate)
                self._status_by_slot[slot].commit_certificate =  commit_certificate

        # Commit once the next slot of the last_commit_slot get commit certificate
        while (str(self._last_commit_slot + 1) in self._status_by_slot and 
                self._status_by_slot[str(self._last_commit_slot + 1)].commit_certificate):
            self._last_commit_slot += 1

        await self._commit_action()

        return web.Response()
        

    async def synchronize(self):
        '''
        Broadcast current checkpoint and all the commit certificate 
        between next slot of the checkpoint and commit upperbound.

        output:
            json_data = {
                'checkpoint': json_data = {
                    'next_slot': self._next_slot
                    'ckpt': json.dumps(ckpt)
                }
                'commit_certificates':commit_certificates
                    (Elements are commit_certificate.to_dict())
            }
        '''
        # TODO: Only send bubble slot message instead of all.
        while 1:
            await asyncio.sleep(self._sync_interval)
            commit_certificates = {}
            for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
                slot = str(i)
                if (slot in self._status_by_slot) and (
                        self._status_by_slot[slot].commit_certificate):
                    status = self._status_by_slot[slot]
                    commit_certificates[slot] = status.commit_certificate.to_dict()
            json_data = {
                'checkpoint': self._ckpt.get_ckpt_info(),
                'commit_certificates':commit_certificates
            }
            await self._post(self._nodes, PBFTHandler.RECEIVE_SYNC, json_data)



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
    parser = argparse.ArgumentParser(description='PBFT Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')
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

    pbft = PBFTHandler(args.index, conf)

    asyncio.ensure_future(pbft.synchronize())

    app = web.Application()
    app.add_routes([
        web.post('/' + PBFTHandler.REQUEST, pbft.get_request),
        web.post('/' + PBFTHandler.PREPREPARE, pbft.preprepare),
        web.post('/' + PBFTHandler.PREPARE, pbft.prepare),
        web.post('/' + PBFTHandler.COMMIT, pbft.commit),
        web.post('/' + PBFTHandler.REPLY, pbft.reply),
        web.post('/' + PBFTHandler.RECEIVE_CKPT_VOTE, pbft.receive_ckpt_vote),
        web.post('/' + PBFTHandler.RECEIVE_SYNC, pbft.receive_sync),
        ])

    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    main()

