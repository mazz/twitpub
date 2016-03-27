from __future__ import print_function
import zmq
import time
import sys
import random
from  multiprocessing import Process
import msgpack
import binascii
import logging
FORMAT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

def main():
    backend = None
    try:
        context = zmq.Context(1)
        # Socket facing clients
        frontend = context.socket(zmq.SUB)
        frontend.bind("tcp://*:5559")
        
        frontend.setsockopt(zmq.SUBSCRIBE, "")
        
        # Socket facing services
        backend = context.socket(zmq.PUB)
        backend.bind("tcp://*:5560")
    
        zmq.device(zmq.FORWARDER, frontend, backend)
    except Exception, e:
        LOG.info(e)
        LOG.info("bringing down zmq device")
    finally:
        pass
        frontend.close()
        if backend is not None:
            backend.close()
        context.term()

if __name__ == "__main__":
    main()