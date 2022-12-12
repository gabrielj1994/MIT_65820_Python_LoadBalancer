import logging
import math
import random
import threading
import time
from urllib.request import Request

import matplotlib.pyplot as plt

from collections import deque
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from multiprocessing.managers import BaseManager, SyncManager
# from multiprocessing import managers.SyncManager
from queue import Queue

from util import MaxHeap

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)


# class GlobalVariables:
#     def __init__(self):
#         # NOTES: Abstraction of network architecture for python-only testing
#         # k-v: device-id:device-instance
#         self.REQUEST_EXECUTION_MS = {'LIGHT': 50, 'MEDIUM': 500, 'HEAVY': 2000}
#         self.DEVICE_TABLE = {}
#         # Structure for tracking the data to populate graph with.
#         # Format: {<rlb_uuid-worker_uuid>: [[x-axis-values], [y-axis-values]]}
#         # Graph: x-axis is epoch time; y-axis is queue size
#         self.GRAPH_DATA = {}
#         self.start_timestamp = time.time_ns() / 1000000.0
#
#         self.BUF_SIZE = 100
#
#         self.MESSAGE_TYPES = {'NLB': {'route_request', 'route_response'},
#                          'RLB': {'route_request', 'route_response', 'work_steal'}}

# NOTES: Abstraction of network architecture for python-only testing
# k-v: device-id:device-instance
REQUEST_EXECUTION_MS = {'LIGHT': 50, 'MEDIUM': 500, 'HEAVY': 2000}
DEVICE_TABLE = {}
# Structure for tracking the data to populate graph with.
# Format: {<rlb_uuid-worker_uuid>: [[x-axis-values], [y-axis-values]]}
# Graph: x-axis is epoch time; y-axis is queue size
GRAPH_DATA = {}
start_timestamp = time.time_ns() / 1000000.0

BUF_SIZE = 100

MESSAGE_TYPES = {'NLB': {'route_request', 'route_response'},
                 'RLB': {'route_request', 'route_response', 'work_steal'}}


'''
                item = self.deque.popleft()
                queue_size = len(self.deque)
                logging.debug('Getting ' + str(item)
                              + ' : ' + str(queue_size) + ' items in queue')

                # NOTE: Prepare LoadHeader
                rlb_uuid = DEVICE_TABLE[self.worker_uuid].load_balancer_uuid
                request_type = list(REQUEST_EXECUTION_MS.keys())[
                                    random.randint(0, len(REQUEST_EXECUTION_MS.keys()) - 1)]
                execution_time_ms = REQUEST_EXECUTION_MS[request_type]
                header = LoadHeader(self.worker_uuid, request_type, queue_size)

                DEVICE_TABLE[rlb_uuid].route_response(header)
'''
class QueueManager:
    def __init__(self, uuid, load_balancer_uuid):
        self.queue_lock = threading.Lock()
        self.uuid = uuid
        # self.listener = ConsumerThread(name="qm_listener_" + str(self.uuid))
        self.queue = deque(maxlen=BUF_SIZE)
        self.load_balancer_uuid = load_balancer_uuid
        self.drain_thread = WorkerThread(deque=self.queue, name="qm_drainQ_" + str(uuid), worker_uuid=self.uuid)
        # self.drain_thread.start()


    def start_workers(self):
        self.drain_thread.start()

    def get_work(self):
        # Validations
        # NOTE: Check if queue empty
        with self.queue_lock:
            # TODO: remove me, no work stealing
            # if len(self.queue) == 0:
            #     return None
            if len(self.queue) <= 2:
                # NOTE: Request work steal async. Should not block, and just return no-op
                # (or a callback of some kind to notify when queue non-empty)
                req = self.request_work_steal()
                if req is None:
                    if len(self.queue) == 0:
                        return None
                else:
                    # self.put_work(req)
                    self.queue.append(req)
    # #%            logging.info('QueueManager get_work op [uuid=%s, queue_size=%d]' % (str(self.uuid), len(self.queue)))
            self.queue.popleft()
        # NOTE: Prepare LoadHeader
        # rlb_uuid = DEVICE_TABLE[self.worker_uuid].load_balancer_uuid
        request_type = list(REQUEST_EXECUTION_MS.keys())[
            random.randint(0, len(REQUEST_EXECUTION_MS.keys()) - 1)]
        execution_time_ms = REQUEST_EXECUTION_MS[request_type]
        timestamp = (time.time_ns() / 1000000.0) - start_timestamp
        # if (15000 > timestamp > 10000 or timestamp > 23000) and (self.uuid == 4 or self.uuid == 8 or self.uuid == 14):
        #     execution_time_ms = 50
        header = LoadHeader(self.uuid, request_type, len(self.queue))
        DEVICE_TABLE[self.load_balancer_uuid].route_response(header)
        time.sleep(execution_time_ms/1000.0)
        # return execution_time_ms
        # GRAPH_DATA[str(self.load_balancer_uuid) + "-" + str(self.uuid)][0].append((time.time_ns()/1000000.0) - start_timestamp)
        # GRAPH_DATA[str(self.load_balancer_uuid) + "-" + str(self.uuid)][1].append(len(self.queue))

    def put_work(self, req):
#%        logging.info('QueueManager put_work op [uuid=%s, queue_size=%d]' % (str(self.uuid), len(self.queue)))
        if len(self.queue) == self.queue.maxlen:
            # NOTE: should not occur
            # TODO: Reply with error message with full queue code
            return None
        self.queue.append(req)
        GRAPH_DATA["lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" + str(self.uuid)][0].append(
                                                                        (time.time_ns() / 1000000.0) - start_timestamp)
        GRAPH_DATA["lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" + str(self.uuid)][1].append(len(self.queue))

    def steal_work(self):
        # Validations
        # NOTE: Check if queue empty
        with self.queue_lock:
            if len(self.queue) <= 2:
                # NOTE: work-steal request comes from load-balancer, if queue empty,
                # load balancer must be informed to steal from another worker. Impact is bounded, since load-balancer should
                # have picked the worker with the longest queue. If longest queue worker is suddenly empty, then likely
                # many available workers are idle, and the network is simply under light load
                return None
            # TODO: locking the queue when popping from it
            # Offer no schedule ordering guarantees.
    #%        logging.info('QueueManager steal_work op [uuid=%s, queue_size=%d]' % (str(self.uuid), len(self.queue)))
            return self.queue.popleft()

    def request_work_steal(self):
        # NOTE: this should be async, and "callback" is effectively treated as simply a new request arriving
        # (not aware it is the requested stolen work).
        return DEVICE_TABLE[self.load_balancer_uuid].work_steal_request(self.uuid)

    def get_queue_state(self):
        return len(self.queue)


# def process_request_queue(request_queue, request_queue_lock, uuid):
def process_request_queue(nlb):
#%    print("Inside executor", flush=True)
    return
    # while True:
    #     # self.queue_lock.acquire()
    #     # acquired_lock = True
    #     if not nlb.request_queue.empty():
    #         print("Getting Item From Queue", flush=True)
    #         item = nlb.request_queue.get()
    #         queue_size = nlb.request_queue.qsize()
    #         # self.queue_lock.release()
    #         # acquired_lock = False
    #         logging.debug('Getting ' + str(item)
    #                       + ' : ' + str(queue_size) + ' items in queue')
    #
    #         if item['request_type'] not in MESSAGE_TYPES['NLB']:
    #             # ERROR: Invalid request type
    #             continue
    #         elif item['request_type'] == 'route_request':
    #             nlb.route_request(item['args'][0])
    #         elif item['request_type'] == 'route_response':
    #             nlb.route_response(item['args'][0])
    #         else:
    #             # ERROR: Unsupported request type
    #             continue
    #     # if acquired_lock:
    #     # self.queue_lock.release()
    # print("Finishing executor", flush=True)

#
# class JIQNLB:
#     def __init__(self, uuid, workers=None, network_load_balancer_uuid=None):
#         self.uuid = uuid
#         self.request_queue = Queue()
#         self.request_queue_lock = threading.Lock()
#         self.response_queue = Queue()
#         self.response_queue_lock = threading.Lock()
#         self.queue_locks = {}
#         self.queue_loads_workers = {}
#         self.current_load = 0
#         self.atomic_lock = threading.Lock()
#         if workers is not None:
#             for uuid in workers:
#                 self.queue_loads_workers[uuid] = 0
#                 self.queue_locks[uuid] = threading.Lock()
#                 # "lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" +
#                 GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(uuid)] = [[0], [0]]
#         self.nlb_uuid = network_load_balancer_uuid
#         self.work_steal_threshold = 3
#         self.steal_candidates = {}
#         self.lb_candidates = {}
#         # self.request_thread = ToRLoadBalancer.RLBThread(queue=self.request_queue, queue_lock=self.request_queue_lock,
#         #                                                 uuid=self.uuid, name="rlb_request_thread_" + str(uuid))
#         # self.request_thread.start()
#         # self.response_thread = ToRLoadBalancer.RLBThread(queue=self.response_queue, queue_lock=self.response_queue_lock,
#         #                                                  uuid=self.uuid, name="rlb_response_thread_" + str(uuid))
#         # self.response_thread.start()
#
#     def route_request_enqueue(self, req):
#         item = {'request_type': 'route_request', 'args': [req]}
#         self.request_queue.put(item)
#
#     def route_request(self, req):
#         #%        logging.info('ToRLoadBalancer route_request op [uuid=%s, load=%d]' % (str(self.uuid), self.current_load))
#         with self.atomic_lock:
#             self.current_load += 1
#         # return self.route_request_random(req)
#         return self.route_request_jiq(req)
#
#     def route_request_jiq(self, req):
#
#     def route_request_random(self, req):
#         # NOTE: random algorithm
#         index = random.randint(0, len(self.queue_loads_workers.keys()) - 1)
#         return DEVICE_TABLE[list(self.queue_loads_workers.keys())[index]].put_work(req)
#
#     def route_request_sq(self, req):
#         # NOTE: route to a short queue
#         # Sort current load sizes
#         sorted_loads = sorted(self.queue_loads_workers.items(), key=lambda x: x[1], reverse=False)
#         # TODO: remove print
#         #%        logging.info('ToRLoadBalancer route_request_sq op [sorted_loads=%s]' % (str(sorted_loads)))
#         # TODO: track in-flight requests
#         self.increment_queue(sorted_loads[0][0])
#         return DEVICE_TABLE[sorted_loads[0][0]].put_work(req)
#
#     def work_steal_request(self, source_uuid):
#         if len(self.steal_candidates) > 0:
#             sorted_loads = sorted(self.steal_candidates.items(), key=lambda x: x[1])
#             timestamp = (time.time_ns() / 1000000.0) - start_timestamp
#             logging.info('ToRLoadBalancer work_steal_request op [sorted_loads=%s, source_uuid=%s, timestamp=%d]'
#                          % (str(sorted_loads), source_uuid, timestamp))
#             self.decrement_queue(sorted_loads[0][0])
#             self.increment_queue(source_uuid)
#             return DEVICE_TABLE[sorted_loads[0][0]].steal_work()
#         else:
#             return None
#
#     def route_response_enqueue(self, load_header):
#         item = {'request_type': 'route_response', 'args': [load_header]}
#         self.response_queue.put(item)
#
#     def route_response(self, load_header):
#         with self.atomic_lock:
#             self.current_load -= 1
#         self.update_network_load_state(load_header.uuid, load_header.queue_size)
#         #%        logging.info('ToRLoadBalancer route_response op [uuid=%s, load=%d]' % (str(self.uuid), self.current_load))
#         # "lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" +
#         GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(load_header.uuid)][0].append(
#             (time.time_ns() / 1000000.0) - start_timestamp)
#         GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(load_header.uuid)][1].append(
#             self.queue_loads_workers[load_header.uuid])
#         load_header.queue_size = self.current_load
#         load_header.uuid = self.uuid
#         DEVICE_TABLE[self.nlb_uuid].route_response(load_header)
#         # TODO: Fix waterfalling of route response messages
#         # DEVICE_TABLE[self.nlb_uuid].route_response_enqueue(load_header)
#
#     def increment_queue(self, uuid):
#         # TODO: improve locking, this is too restrictive, should be just on uuid
#         with self.queue_locks[uuid]:
#             self.queue_loads_workers[uuid] += 1
#             if self.queue_loads_workers[uuid] >= self.work_steal_threshold \
#                     and (uuid in self.steal_candidates.keys() or len(self.steal_candidates) < 3):
#                 self.steal_candidates[uuid] = self.queue_loads_workers[uuid]
#
#     def decrement_queue(self, uuid):
#         # TODO: improve locking, this is too restrictive, should be just on uuid
#         with self.queue_locks[uuid]:
#             self.queue_loads_workers[uuid] -= 1
#             if uuid in self.steal_candidates.keys():
#                 #%                logging.info('ToRLoadBalancer decrement_queue op [steal_candidates=%s]'
#                              #% % (str(list(self.steal_candidates.items()))))
#                 if self.queue_loads_workers[uuid] < self.work_steal_threshold:
#                     self.steal_candidates.pop(uuid)
#                 else:
#                     self.steal_candidates[uuid] = self.queue_loads_workers[uuid]
#
#     def update_network_load_state(self, worker_uuid, queue_size):
#         with self.queue_locks[worker_uuid]:
#             self.queue_loads_workers[worker_uuid] = queue_size
#             if worker_uuid in self.steal_candidates.keys():
#                 # logging.info('ToRLoadBalancer update_network_load_state op [steal_candidates=%s]'
#                 #              % (str(list(self.steal_candidates.items()))))
#                 if self.queue_loads_workers[worker_uuid] < self.work_steal_threshold:
#                     self.steal_candidates.pop(worker_uuid)
#                 else:
#                     self.steal_candidates[worker_uuid] = self.queue_loads_workers[worker_uuid]
#
#     def heartbeat(self):
#         pass
#
#     def add_worker(self, worker_uuid, queue_load=0):
#         self.queue_loads_workers[worker_uuid] = queue_load
#         self.queue_locks[worker_uuid] = threading.Lock()
#         # "lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" +
#
#         GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(worker_uuid)] = [[0], [0]]
#
#     def set_network_load_balancer(self, nlb_uuid):
#         self.nlb_uuid = nlb_uuid

class NetworkLoadBalancer:
    def __init__(self, uuid, rack_load_balancers=None, network_load_balancers=None):
        self.uuid = uuid
        self.request_queue = Queue()
        self.request_queue_lock = threading.Lock()
        self.response_queue = Queue()
        self.response_queue_lock = threading.Lock()
        self.queue_loads_rlb = {}
        self.queue_locks = {}
        if rack_load_balancers is not None:
            for key in rack_load_balancers:
                self.queue_loads_rlb[key] = 0
                self.queue_locks[uuid] = threading.Lock()

        self.network_load_balancers = []
        if network_load_balancers is not None:
            self.network_load_balancers = network_load_balancers
        # self.prepare_process_executors()
        # self.work_request_thread = NetworkLoadBalancer.NLBThread(queue=self.request_queue,
        #                                                          queue_lock=self.request_queue_lock,
        #                                                          uuid=self.uuid, name="nlb_request_thread_"+str(uuid))
        # self.work_request_thread.start()
        # self.work_response_thread = NetworkLoadBalancer.NLBThread(queue=self.response_queue,
        #                                                           queue_lock=self.response_queue_lock,
        #                                                           uuid=self.uuid, name="nlb_response_thread_"+str(uuid))
        # self.work_response_thread.start()

    # def process_request_queue(nlb):
    #     print("Starting executor", flush=True)
    #     while True:
    #         # self.queue_lock.acquire()
    #         # acquired_lock = True
    #         if not nlb.request_queue.empty():
    #             print("Getting Item From Queue", flush=True)
    #             item = nlb.request_queue.get()
    #             queue_size = nlb.request_queue.qsize()
    #             # self.queue_lock.release()
    #             # acquired_lock = False
    #             logging.debug('Getting ' + str(item)
    #                           + ' : ' + str(queue_size) + ' items in queue')
    #
    #             if item['request_type'] not in MESSAGE_TYPES['NLB']:
    #                 # ERROR: Invalid request type
    #                 continue
    #             elif item['request_type'] == 'route_request':
    #                 nlb.route_request(item['args'][0])
    #             elif item['request_type'] == 'route_response':
    #                 nlb.route_response(item['args'][0])
    #             else:
    #                 # ERROR: Unsupported request type
    #                 continue
    #         # if acquired_lock:
    #         # self.queue_lock.release()
    #     print("Finishing executor", flush=True)

    def prepare_process_executors(self):
        executor = ProcessPoolExecutor(max_workers=1)
#%        print("Starting executor", flush=True)
        # executor.submit(process_request_queue, self.request_queue, self.request_queue_lock, self.uuid)
        executor.submit(process_request_queue, self)

    def route_request_enqueue(self, req):
        item = {'request_type': 'route_request', 'args': [req]}
        #%        logging.info('NetworkLoadBalancer route_request op [uuid=%s]' % (str(self.uuid)))
        self.request_queue.put(item)

    def route_request(self, req):
        #%        logging.info('NetworkLoadBalancer route_request op [uuid=%s]' % (str(self.uuid)))
        # return self.route_request_random(req)
        return self.route_request_sq(req)

    def route_request_random(self, req):
        # NOTE: random lb algorithm
        index = random.randint(0, len(self.queue_loads_rlb.keys())-1)
        return DEVICE_TABLE[list(self.queue_loads_rlb.keys())[index]].route_request(req)
        # return DEVICE_TABLE[list(self.queue_loads_rlb.keys())[index]].route_request_enqueue(req)

    def route_request_sq(self, req):
        # NOTE: route to a short queue
        # Sort current load sizes
        sorted_loads = sorted(self.queue_loads_rlb.items(), key=lambda x: x[1], reverse=False)
        # TODO: remove print
        #%        logging.info('NetworkLoadBalancer route_request_sq op [sorted_loads=%s]' % (str(sorted_loads)))
        # TODO: track in-flight requests
        self.increment_queue(sorted_loads[0][0])
        return DEVICE_TABLE[sorted_loads[0][0]].route_request(req)
        # return DEVICE_TABLE[sorted_loads[0][0]].route_request_enqueue(req)

    def route_response_enqueue(self, load_header):
        item = {'request_type': 'route_response', 'args': [load_header]}
        self.response_queue.put(item)

    def route_response(self, load_header):
        self.update_network_load_state(load_header.uuid, load_header.queue_size)

    def update_network_load_state(self, rlb_uuid, queue_size):
        self.queue_loads_rlb[rlb_uuid] = queue_size

    def increment_queue(self, uuid):
        # TODO: improve locking, this is too restrictive, should be just on uuid
        with self.queue_locks[uuid]:
            self.queue_loads_rlb[uuid] += 1

    def decrement_queue(self, uuid):
        # TODO: improve locking, this is too restrictive, should be just on uuid
        with self.queue_locks[uuid]:
            self.queue_loads_rlb[uuid] -= 1

    def heartbeat(self):
        pass

    def add_rack_load_balancer(self, rlb_uuid, queue_load=0):
        self.queue_loads_rlb[rlb_uuid] = queue_load
        self.queue_locks[rlb_uuid] = threading.Lock()

    def add_network_load_balancer(self, nlb_uuid):
        self.network_load_balancers.append(nlb_uuid)

    class NLBThread(threading.Thread):
        def __init__(self, queue, queue_lock, uuid, group=None, target=None, name=None,
                     args=(), kwargs=None, verbose=None):
            super(NetworkLoadBalancer.NLBThread, self).__init__()
            self.target = target
            self.name = name
            self.queue = queue
            self.queue_lock = queue_lock
            self.uuid = uuid
            return

        def run(self):
            while True:
                # self.queue_lock.acquire()
                # acquired_lock = True
                if not self.queue.empty():
                    item = self.queue.get()
                    queue_size = self.queue.qsize()
                    # self.queue_lock.release()
                    # acquired_lock = False
                    #%logging.debug('Getting ' + str(item)
                    #               + ' : ' + str(queue_size) + ' items in queue')

                    if item['request_type'] not in MESSAGE_TYPES['NLB']:
                        # ERROR: Invalid request type
                        continue
                    elif item['request_type'] == 'route_request':
                        DEVICE_TABLE[self.uuid].route_request(item['args'][0])
                    elif item['request_type'] == 'route_response':
                        DEVICE_TABLE[self.uuid].route_response(item['args'][0])
                    else:
                        # ERROR: Unsupported request type
                        continue
                # if acquired_lock:
                    # self.queue_lock.release()


class ToRLoadBalancer:
    def __init__(self, uuid, workers=None, network_load_balancer_uuid=None):
        self.uuid = uuid
        self.request_queue = Queue()
        self.request_queue_lock = threading.Lock()
        self.response_queue = Queue()
        self.response_queue_lock = threading.Lock()
        self.queue_locks = {}
        self.queue_loads_workers = {}
        self.current_load = 0
        self.atomic_lock = threading.Lock()
        if workers is not None:
            for uuid in workers:
                self.queue_loads_workers[uuid] = 0
                self.queue_locks[uuid] = threading.Lock()
                # "lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" +
                GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(uuid)] = [[0], [0]]
        self.nlb_uuid = network_load_balancer_uuid
        self.work_steal_threshold = 3
        self.steal_candidates = {}
        self.lb_candidates = {}
        # self.request_thread = ToRLoadBalancer.RLBThread(queue=self.request_queue, queue_lock=self.request_queue_lock,
        #                                                 uuid=self.uuid, name="rlb_request_thread_" + str(uuid))
        # self.request_thread.start()
        # self.response_thread = ToRLoadBalancer.RLBThread(queue=self.response_queue, queue_lock=self.response_queue_lock,
        #                                                  uuid=self.uuid, name="rlb_response_thread_" + str(uuid))
        # self.response_thread.start()

    def route_request_enqueue(self, req):
        item = {'request_type': 'route_request', 'args': [req]}
        self.request_queue.put(item)

    def route_request(self, req):
        #%        logging.info('ToRLoadBalancer route_request op [uuid=%s, load=%d]' % (str(self.uuid), self.current_load))
        with self.atomic_lock:
            self.current_load += 1
        # return self.route_request_random(req)
        return self.route_request_sq(req)

    def route_request_random(self, req):
        # NOTE: random algorithm
        sorted_loads = sorted(self.queue_loads_workers.items(), key=lambda x: x[1], reverse=False)
        # index = random.randint(0, len(self.queue_loads_workers.keys()) - 1)
        # return DEVICE_TABLE[list(self.queue_loads_workers.keys())[index]].put_work(req)
        index = random.randint(0, len(self.queue_loads_workers.keys()) - 2)
        if index == 0:
            random_var = random.randint(0, 9)
            if random_var < 3:
                index += 2
        self.increment_queue(sorted_loads[index][0])
        return DEVICE_TABLE[sorted_loads[index][0]].put_work(req)


    def route_request_sq(self, req):
        # NOTE: route to a short queue
        # Sort current load sizes
        sorted_loads = sorted(self.queue_loads_workers.items(), key=lambda x: x[1], reverse=False)
        # TODO: remove print
        #%        logging.info('ToRLoadBalancer route_request_sq op [sorted_loads=%s]' % (str(sorted_loads)))
        # TODO: track in-flight requests
        self.increment_queue(sorted_loads[0][0])
        return DEVICE_TABLE[sorted_loads[0][0]].put_work(req)

    def work_steal_request(self, source_uuid):
        # TODO: remove me. jiq test
        # return None
        if len(self.steal_candidates) > 0:
            sorted_loads = sorted(self.queue_loads_workers.items(), key=lambda x: x[1], reverse=True)
            timestamp = (time.time_ns() / 1000000.0) - start_timestamp
            logging.info('ToRLoadBalancer work_steal_request op [sorted_loads=%s, source_uuid=%s, timestamp=%d]'
                         % (str(sorted_loads), source_uuid, timestamp))
            self.decrement_queue(sorted_loads[0][0])
            GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(sorted_loads[0][0])][0].append(
                timestamp)
            GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(sorted_loads[0][0])][1].append(
                self.queue_loads_workers[sorted_loads[0][0]])
            self.increment_queue(source_uuid)
            GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(source_uuid)][0].append(
                timestamp)
            GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(source_uuid)][1].append(
                self.queue_loads_workers[source_uuid])
            return DEVICE_TABLE[sorted_loads[0][0]].steal_work()
        else:
            return None

    def route_response_enqueue(self, load_header):
        item = {'request_type': 'route_response', 'args': [load_header]}
        self.response_queue.put(item)

    def route_response(self, load_header):
        with self.atomic_lock:
            self.current_load -= 1
        self.update_network_load_state(load_header.uuid, load_header.queue_size)
        #%        logging.info('ToRLoadBalancer route_response op [uuid=%s, load=%d]' % (str(self.uuid), self.current_load))
        # "lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" +
        GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(load_header.uuid)][0].append(
            (time.time_ns() / 1000000.0) - start_timestamp)
        GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(load_header.uuid)][1].append(
            self.queue_loads_workers[load_header.uuid])
        load_header.queue_size = self.current_load
        load_header.uuid = self.uuid
        DEVICE_TABLE[self.nlb_uuid].route_response(load_header)
        # TODO: Fix waterfalling of route response messages
        # DEVICE_TABLE[self.nlb_uuid].route_response_enqueue(load_header)

    def increment_queue(self, uuid):
        # TODO: improve locking, this is too restrictive, should be just on uuid
        with self.queue_locks[uuid]:
            self.queue_loads_workers[uuid] += 1
            if self.queue_loads_workers[uuid] >= self.work_steal_threshold:
                self.steal_candidates[uuid] = self.queue_loads_workers[uuid]

    def decrement_queue(self, uuid):
        # TODO: improve locking, this is too restrictive, should be just on uuid
        with self.queue_locks[uuid]:
            self.queue_loads_workers[uuid] -= 1
            if uuid in self.steal_candidates.keys():
                #%                logging.info('ToRLoadBalancer decrement_queue op [steal_candidates=%s]'
                             #% % (str(list(self.steal_candidates.items()))))
                if self.queue_loads_workers[uuid] < self.work_steal_threshold:
                    self.steal_candidates.pop(uuid)
                else:
                    self.steal_candidates[uuid] = self.queue_loads_workers[uuid]

    def update_network_load_state(self, worker_uuid, queue_size):
        with self.queue_locks[worker_uuid]:
            self.queue_loads_workers[worker_uuid] = queue_size
            if worker_uuid in self.steal_candidates.keys():
                # logging.info('ToRLoadBalancer update_network_load_state op [steal_candidates=%s]'
                #              % (str(list(self.steal_candidates.items()))))
                if self.queue_loads_workers[worker_uuid] < self.work_steal_threshold:
                    self.steal_candidates.pop(worker_uuid)
                else:
                    self.steal_candidates[worker_uuid] = self.queue_loads_workers[worker_uuid]

    def heartbeat(self):
        pass

    def add_worker(self, worker_uuid, queue_load=0):
        self.queue_loads_workers[worker_uuid] = queue_load
        self.queue_locks[worker_uuid] = threading.Lock()
        # "lbid-" + str(self.load_balancer_uuid) + "-" + "wkid-" +

        GRAPH_DATA["lbid-" + str(self.uuid) + "-" + "wkid-" + str(worker_uuid)] = [[0], [0]]

    def set_network_load_balancer(self, nlb_uuid):
        self.nlb_uuid = nlb_uuid

    class RLBThread(threading.Thread):
        def __init__(self, queue, queue_lock, uuid, group=None, target=None, name=None,
                     args=(), kwargs=None, verbose=None):
            super(ToRLoadBalancer.RLBThread, self).__init__()
            self.target = target
            self.name = name
            self.queue = queue
            self.queue_lock = queue_lock
            self.uuid = uuid
            return

        def run(self):
            while True:
                self.queue_lock.acquire()
                acquired_lock = True
                if not self.queue.empty():
                    item = self.queue.get()
                    queue_size = self.queue.qsize()
                    self.queue_lock.release()
                    acquired_lock = False
                    #% logging.debug('Getting ' + str(item)
                    #               + ' : ' + str(queue_size) + ' items in queue')

                    if item['request_type'] not in MESSAGE_TYPES['NLB']:
                        # ERROR: Invalid request type
                        continue
                    elif item['request_type'] == 'route_request':
                        DEVICE_TABLE[self.uuid].route_request(item['args'][0])
                    elif item['request_type'] == 'route_response':
                        DEVICE_TABLE[self.uuid].route_response(item['args'][0])
                    else:
                        # ERROR: Unsupported request type
                        continue
                if acquired_lock:
                    self.queue_lock.release()


# class ProducerThread(threading.Thread):
#     def __init__(self, group=None, target=None, name=None,
#                  args=(), kwargs=None, verbose=None):
#         super(ProducerThread, self).__init__()
#         self.target = target
#         self.name = name
#
#     def run(self):
#         while True:
#             if not q.full():
#                 item = random.randint(1,10)
#                 q.put(item)
#                 logging.debug('Putting ' + str(item)
#                               + ' : ' + str(len(q)) + ' items in queue')
#                 time.sleep(random.random())
#         return


# class ConsumerThread(threading.Thread):
#     def __init__(self, group=None, target=None, name=None,
#                  args=(), kwargs=None, verbose=None):
#         super(ConsumerThread, self).__init__()
#         # self.target = target
#         self.name = name
#         return
#
#     def run(self):
#         while True:
#             if not q.empty():
#                 item = q.get()
#                 logging.debug('Getting ' + str(item)
#                               + ' : ' + str(q.qsize()) + ' items in queue')
#                 time.sleep(random.random())
#         return


class WorkerThread(threading.Thread):
    def __init__(self, deque, worker_uuid, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(WorkerThread, self).__init__()
        self.target = target
        self.name = name
        self.deque = deque
        self.worker_uuid = worker_uuid
        return

    def run(self):
        while True:
            # if len(self.deque) > 0:
            DEVICE_TABLE[self.worker_uuid].get_work()
                # =================================================================
                # item = self.deque.popleft()
                # queue_size = len(self.deque)
                # logging.debug('Getting ' + str(item)
                #               + ' : ' + str(queue_size) + ' items in queue')
                #
                # # NOTE: Prepare LoadHeader
                # rlb_uuid = DEVICE_TABLE[self.worker_uuid].load_balancer_uuid
                # request_type = list(REQUEST_EXECUTION_MS.keys())[
                #                     random.randint(0, len(REQUEST_EXECUTION_MS.keys()) - 1)]
                # execution_time_ms = REQUEST_EXECUTION_MS[request_type]
                # header = LoadHeader(self.worker_uuid, request_type, queue_size)
                #
                # DEVICE_TABLE[rlb_uuid].route_response(header)
            # if resp is not None:
            #     time.sleep(0.5)  # Sleep 0.5 seconds
                # time.sleep(execution_time_ms/1000.0)  # Sleep for execution_time


class RequestThread(threading.Thread):
    def __init__(self, nlb_uuid, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(RequestThread, self).__init__()
        self.target = target
        self.name = name
        self.deque = deque
        self.nlb_uuid = nlb_uuid
        return

    def run(self):
        count = 0
        while count < 250:
            count += 1
            request = "Dummy Request"
            # DEVICE_TABLE[nlb_uuid].route_request_enqueue(request)
            DEVICE_TABLE[nlb_uuid].route_request(request)
            if count % 5 == 0:
                if 120 > count > 50:
                    time.sleep(0.15)
                elif 180 > count > 120:
                    time.sleep(1)
                elif count > 180:
                    time.sleep(0.35)
                else:
                    time.sleep(0.05)
            elif count % 100 == 0:
                time.sleep(2.5)



# class WorkStealThread(threading.Thread):

# class NLBRouteThread


class LoadHeader:
    def __init__(self, uuid, request_type, queue_size):
        self.uuid = uuid
        self.request_type = request_type
        self.queue_size = queue_size


class MyCustomClass:
    # constructor
    def __init__(self, data):
        # store the data in the instance
        self.data = data
        self.storage = list()

    # do something with the data
    def task(self):
        # generate a random number
        value = random()
        # block for a moment
        time.sleep(value)
        # calculate a new value
        new_value = self.data * value
        # store everything
        self.storage.append((self.data, value, new_value))
        # return the new value
        return new_value

    # get all stored values
    def get_storage(self):
        return self.storage

class CustomManager(BaseManager):
    pass


if __name__ == '__main__':
    # CustomManager.register('MyCustomClass', MyCustomClass)
    # CustomManager.register('LoadHeader', LoadHeader)
    # CustomManager.register('NetworkLoadBalancer', NetworkLoadBalancer)
    # CustomManager.register('GlobalVariables', GlobalVariables)
    # manager = CustomManager()
    # manager.start()
    # shared_custom = manager.MyCustomClass(10)
    # shared_header = manager.LoadHeader(1, "dummy", 1)
    # shared_global = manager.GlobalVariables()
    # shared_nlb = manager.NetworkLoadBalancer(1, [], [])
    # shared_nlb.prepare_process_executors()
    # executor = ProcessPoolExecutor(max_workers=1)
    # print("Starting executor", flush=True)
    # # executor.submit(process_request_queue, self.request_queue, self.request_queue_lock, self.uuid)
    # executor.submit(process_request_queue, shared_nlb)
    # print("Testing")
    # time.sleep(10)
    # manager.shutdown()
    # exit(0)
    nlb_uuid = 1
    DEVICE_TABLE[nlb_uuid] = NetworkLoadBalancer(nlb_uuid, [], [])
    num_rlb = 3
    num_qm = 4
    offset = 2

    for index_rlb in range(num_rlb):
        rlb_uuid = offset + index_rlb*(num_qm+1)
        DEVICE_TABLE[rlb_uuid] = ToRLoadBalancer(rlb_uuid, [], nlb_uuid)
        DEVICE_TABLE[nlb_uuid].add_rack_load_balancer(rlb_uuid)
        for index_qm in range(num_qm):
            qm_uuid = rlb_uuid + 1 + index_qm
            DEVICE_TABLE[qm_uuid] = QueueManager(qm_uuid, rlb_uuid)
            DEVICE_TABLE[rlb_uuid].add_worker(qm_uuid)
            DEVICE_TABLE[qm_uuid].start_workers()

    logging.info('[DEVICE_TABLE=%s]' % str(DEVICE_TABLE))

    request_thread_1 = RequestThread(1, name="driver_thread_1")
    request_thread_2 = RequestThread(1, name="driver_thread_2")
    time.sleep(4)
    request_thread_1.start()
    request_thread_2.start()
    # count = 0
    # while count < 200:
    #     count += 1
    #     request = "Dummy Request"
    #     # DEVICE_TABLE[1].route_request_enqueue(request)
    #     DEVICE_TABLE[1].route_request(request)
    #     if count % 5 == 0:
    #         time.sleep(1.0)
    request_thread_1.join()
    request_thread_2.join()
    time.sleep(60)
    logging.info("[Graph_Data=%s]", str(GRAPH_DATA.items()))
    # NOTES: Graphs
    for line, data in GRAPH_DATA.items():
        x = data[0]
        y = data[1]
        plt.plot(x, y, label=line)

    plt.xlabel("Timestamp (ms from start time)")
    plt.ylabel("Queue Size")
    plt.legend(loc='upper right')
    plt.show()
