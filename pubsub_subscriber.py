from __future__ import print_function
import zmq
import time
import sys
import random
from  multiprocessing import Process
import msgpack
import binascii
import logging
from rmq_producer import ExamplePublisher

FORMAT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def main():
    # Socket to talk to publisher
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    LOG.info("Collecting updates from publisher...")
    socket.connect ("tcp://localhost:%s" % '5560')
    topicfilter = ""
    socket.setsockopt(zmq.SUBSCRIBE, topicfilter)

    example = ExamplePublisher('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600')

    while True:
        if example._publishing:
            packed = socket.recv()
            unpacked = msgpack.unpackb(packed)
            LOG.info('recv unpacked message: %s' % repr(unpacked))
            example.publish_message(unpacked)    

if __name__ == "__main__":
    main()
