import logging
import math
import random
import threading
import time
import matplotlib.pyplot as plt

from queue import Queue
from collections import deque

from util import MaxHeap

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)

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
q = Queue(BUF_SIZE)
dq = deque(maxlen=BUF_SIZE)


class QueueManager:
    def __init__(self, uuid, load_balancer_uuid):
        self.uuid = uuid
        self.listener = ConsumerThread(name="qm_listener_" + str(self.uuid))
        self.queue = deque(maxlen=BUF_SIZE)
        self.load_balancer_uuid = load_balancer_uuid
        self.drain_thread = WorkerThread(deque=self.queue, name="qm_drainQ_" + str(uuid), worker_uuid=self.uuid)
        self.drain_thread.start()

    def get_work(self):
        logging.info('QueueManager get_work op [uuid=%s, queue_size=%d]' % (str(self.uuid), len(self.queue)))
        # Validations
        # NOTE: Check if queue empty
        if len(self.queue) == 0:
            # NOTE: Request work steal async. Should not block, and just return no-op
            # (or a callback of some kind to notify when queue non-empty)
            self.request_work_steal()
            return None
        self.queue.popleft()
        GRAPH_DATA[str(self.load_balancer_uuid) + "-" + str(self.uuid)][0].append((time.time_ns()/1000000.0) - start_timestamp)
        GRAPH_DATA[str(self.load_balancer_uuid) + "-" + str(self.uuid)][1].append(len(self.queue))

    def put_work(self, req):
        logging.info('QueueManager put_work op [uuid=%s, queue_size=%d]' % (str(self.uuid), len(self.queue)))
        if len(self.queue) == self.queue.maxlen:
            # NOTE: should not occur
            # TODO: Reply with error message with full queue code
            return None
        self.queue.append(req)
        GRAPH_DATA[str(self.load_balancer_uuid) + "-" + str(self.uuid)][0].append((time.time_ns() / 1000000.0) - start_timestamp)
        GRAPH_DATA[str(self.load_balancer_uuid) + "-" + str(self.uuid)][1].append(len(self.queue))

    def steal_work(self):
        # Validations
        # NOTE: Check if queue empty
        if len(self.queue) == 0:
            # NOTE: work-steal request comes from load-balancer, if queue empty,
            # load balancer must be informed to steal from another worker. Impact is bounded, since load-balancer should
            # have picked the worker with the longest queue. If longest queue worker is suddenly empty, then likely
            # many available workers are idle, and the network is simply under light load
            return None
        # TODO: locking the queue when popping from it
        # NOTE: pop from far end of queue. Slightly "unfair" due to later-arriving requests being serviced earlier.
        # Offer no schedule ordering guarantees.
        return self.queue.popleft()

    def request_work_steal(self):
        # NOTE: this should be async, and "callback" is effectively treated as simply a new request arriving
        # (not aware it is the requested stolen work).
        DEVICE_TABLE[self.load_balancer_uuid].work_steal_request()

    def get_queue_state(self):
        return len(self.queue)


class NetworkLoadBalancer:
    def __init__(self, uuid, rack_load_balancers=None, network_load_balancers=None):
        self.uuid = uuid
        self.queue_loads_rlb = {}
        self.queue_locks = {}
        if rack_load_balancers is not None:
            for key in rack_load_balancers:
                self.queue_loads_rlb[key] = 0
                self.queue_locks[uuid] = threading.Lock()

        self.network_load_balancers = []
        if network_load_balancers is not None:
            self.network_load_balancers = network_load_balancers

    def route_request(self, req):
        logging.info('NetworkLoadBalancer route_request op [uuid=%s]' % (str(self.uuid)))
        # return self.route_request_random(req)
        return self.route_request_sq(req)

    def route_request_random(self, req):
        # NOTE: random lb algorithm
        index = random.randint(0, len(self.queue_loads_rlb.keys())-1)
        return DEVICE_TABLE[list(self.queue_loads_rlb.keys())[index]].route_request(req)

    def route_request_sq(self, req):
        # NOTE: route to a short queue
        # Sort current load sizes
        sorted_loads = sorted(self.queue_loads_rlb.items(), key=lambda x: x[1], reverse=False)
        # TODO: remove print
        logging.info('NetworkLoadBalancer route_request_sq op [sorted_loads=%s]' % (str(sorted_loads)))
        # TODO: track in-flight requests
        self.increment_queue(sorted_loads[0][0])
        return DEVICE_TABLE[sorted_loads[0][0]].route_request(req)

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


class ToRLoadBalancer:
    def __init__(self, uuid, workers=None, network_load_balancer_uuid=None):
        self.uuid = uuid
        self.queue_locks = {}
        self.queue_loads_workers = {}
        self.current_load = 0
        self.atomic_lock = threading.Lock()
        if workers is not None:
            for uuid in workers:
                self.queue_loads_workers[uuid] = 0
                self.queue_locks[uuid] = threading.Lock()
                GRAPH_DATA[str(self.uuid) + "-" + str(uuid)] = [[0], [0]]

        self.nlb_uuid = network_load_balancer_uuid

    def route_request(self, req):
        logging.info('ToRLoadBalancer route_request op [uuid=%s, load=%d]' % (str(self.uuid), self.current_load))
        with self.atomic_lock:
            self.current_load += 1
        return self.route_request_random(req)
        # return self.route_request_sq(req)

    def route_request_random(self, req):
        # NOTE: random algorithm
        index = random.randint(0, len(self.queue_loads_workers.keys()) - 1)
        return DEVICE_TABLE[list(self.queue_loads_workers.keys())[index]].put_work(req)

    def route_request_sq(self, req):
        # NOTE: route to a short queue
        # Sort current load sizes
        sorted_loads = sorted(self.queue_loads_workers.items(), key=lambda x: x[1], reverse=False)
        # TODO: remove print
        logging.info('ToRLoadBalancer route_request_sq op [sorted_loads=%s]' % (str(sorted_loads)))
        # TODO: track in-flight requests
        self.increment_queue(sorted_loads[0][0])
        return DEVICE_TABLE[sorted_loads[0][0]].put_work(req)

    def work_steal_request(self):
        pass

    def route_response(self, load_header):
        with self.atomic_lock:
            self.current_load -= 1
        self.update_network_load_state(load_header.uuid, load_header.queue_size)
        logging.info('ToRLoadBalancer route_response op [uuid=%s, load=%d]' % (str(self.uuid), self.current_load))
        GRAPH_DATA[str(self.uuid) + "-" + str(load_header.uuid)][0].append(
            (time.time_ns() / 1000000.0) - start_timestamp)
        GRAPH_DATA[str(self.uuid) + "-" + str(load_header.uuid)][1].append(
            self.queue_loads_workers[load_header.uuid])
        load_header.queue_size = self.current_load
        load_header.uuid = self.uuid
        DEVICE_TABLE[self.nlb_uuid].route_response(load_header)

    def increment_queue(self, uuid):
        # TODO: improve locking, this is too restrictive, should be just on uuid
        with self.queue_locks[uuid]:
            self.queue_loads_workers[uuid] += 1

    def decrement_queue(self, uuid):
        # TODO: improve locking, this is too restrictive, should be just on uuid
        with self.queue_locks[uuid]:
            self.queue_loads_workers[uuid] -= 1

    def update_network_load_state(self, worker_uuid, queue_size):
        self.queue_loads_workers[worker_uuid] = queue_size

    def heartbeat(self):
        pass

    def add_worker(self, worker_uuid, queue_load=0):
        self.queue_loads_workers[worker_uuid] = queue_load
        self.queue_locks[worker_uuid] = threading.Lock()
        GRAPH_DATA[str(self.uuid) + "-" + str(worker_uuid)] = [[0], [0]]

    def set_network_load_balancer(self, nlb_uuid):
        self.nlb_uuid = nlb_uuid


class ProducerThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ProducerThread, self).__init__()
        self.target = target
        self.name = name

    def run(self):
        while True:
            if not q.full():
                item = random.randint(1,10)
                q.put(item)
                logging.debug('Putting ' + str(item)  
                              + ' : ' + str(len(q)) + ' items in queue')
                time.sleep(random.random())
        return


class ConsumerThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ConsumerThread, self).__init__()
        # self.target = target
        self.name = name
        return

    def run(self):
        while True:
            if not q.empty():
                item = q.get()
                logging.debug('Getting ' + str(item) 
                              + ' : ' + str(q.qsize()) + ' items in queue')
                time.sleep(random.random())
        return


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
            if len(self.deque) > 0:
                item = self.deque.popleft()
                queue_size = len(self.deque)
                logging.debug('Getting ' + str(item)
                              + ' : ' + str(queue_size) + ' items in queue')

                # NOTE: Prepare LoadHeader
                rlb_uuid = DEVICE_TABLE[self.worker_uuid].load_balancer_uuid
                request_type = list(REQUEST_EXECUTION_MS.keys())[random.randint(0, len(REQUEST_EXECUTION_MS.keys()) - 1)]
                execution_time_ms = REQUEST_EXECUTION_MS[request_type]
                header = LoadHeader(self.worker_uuid, request_type, queue_size)

                DEVICE_TABLE[rlb_uuid].route_response(header)
                time.sleep(0.5)  # Sleep 0.5 seconds
                # time.sleep(execution_time_ms/1000.0)  # Sleep for execution_time

# class WorkStealThread(threading.Thread):


class LoadHeader:
    # def __init__(self, worker_uuid, request_type, queue_size, rlb_queues=None):
    def __init__(self, uuid, request_type, queue_size):
        self.uuid = uuid
        self.request_type = request_type
        self.queue_size = queue_size
        # self.rlb_queues = None


if __name__ == '__main__':
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

    logging.info('[DEVICE_TABLE=%s]' % str(DEVICE_TABLE))
    # DEVICE_TABLE[2] = ToRLoadBalancer(2, [], DEVICE_TABLE[1].uuid)
    # DEVICE_TABLE[3] = QueueManager(3, DEVICE_TABLE[2].uuid)
    # DEVICE_TABLE[4] = QueueManager(4, DEVICE_TABLE[2].uuid)
    # DEVICE_TABLE[3] = QueueManager(5, DEVICE_TABLE[2].uuid)
    # DEVICE_TABLE[4] = QueueManager(6, DEVICE_TABLE[2].uuid)
    # DEVICE_TABLE[2].add_worker(DEVICE_TABLE[3].uuid)
    # DEVICE_TABLE[2].add_worker(DEVICE_TABLE[4].uuid)
    # DEVICE_TABLE[2].add_worker(DEVICE_TABLE[5].uuid)
    # DEVICE_TABLE[2].add_worker(DEVICE_TABLE[6].uuid)
    # DEVICE_TABLE[2].set_network_load_balancer(DEVICE_TABLE[1].uuid)
    #
    # DEVICE_TABLE[5] = ToRLoadBalancer(5, [], DEVICE_TABLE[1].uuid)
    # DEVICE_TABLE[6] = QueueManager(6, DEVICE_TABLE[5].uuid)
    # DEVICE_TABLE[7] = QueueManager(7, DEVICE_TABLE[5].uuid)
    #
    #
    #
    # DEVICE_TABLE[5].add_worker(DEVICE_TABLE[6].uuid)
    # DEVICE_TABLE[5].add_worker(DEVICE_TABLE[7].uuid)
    # DEVICE_TABLE[5].set_network_load_balancer(DEVICE_TABLE[1].uuid)
    #
    # DEVICE_TABLE[1].add_rack_load_balancer(DEVICE_TABLE[2].uuid)
    # DEVICE_TABLE[1].add_rack_load_balancer(DEVICE_TABLE[5].uuid)

    # nlbs = [DEVICE_TABLE[1]]
    # rlbs = [DEVICE_TABLE[2]]
    # qms = [DEVICE_TABLE[3], DEVICE_TABLE[4]]

    time.sleep(4)

    count = 0
    while count < 200:
        count += 1
        request = "Dummy Request"
        DEVICE_TABLE[1].route_request(request)
        if count % 5 == 0:
            time.sleep(0.10)

    time.sleep(15)
    logging.info("[Graph_Data=%s]", str(GRAPH_DATA.items()))
    # NOTES: Graphs
    for line, data in GRAPH_DATA.items():
        x = data[0]
        y = data[1]
        plt.plot(x, y, label=line)

    plt.xlabel("Timestamp")
    plt.ylabel("Queue Size")
    plt.legend()
    plt.show()

    # line 1 points
    # x1 = [1, 2, 3]
    # y1 = [2, 4, 1]
    # # plotting the line 1 points
    # plt.plot(x1, y1, label="line 1")
    #
    # # line 2 points
    # x2 = [1, 2, 3]
    # y2 = [4, 1, 3]
    # # plotting the line 2 points
    # plt.plot(x2, y2, label="line 2")
    #
    # # naming the x axis
    # plt.xlabel('x - axis')
    # # naming the y axis
    # plt.ylabel('y - axis')
    # # giving a title to my graph
    # plt.title('Two lines on same graph!')
    #
    # # show a legend on the plot
    # plt.legend()
    #
    # # function to show the plot
    # plt.show()
    
    # p = ProducerThread(name='producer')
    # c = ConsumerThread(name='consumer')
    #
    # p.start()
    # time.sleep(2)
    # c.start()
    # time.sleep(2)
