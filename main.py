

import signal
import sys
from broker import Broker
from server import RPCServer

def signal_handler(signal, frame):
    print('Terminating server...')
    rpc_server.close()

rpc_server = RPCServer(Broker(), ('localhost', 8000))

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    rpc_server.listen()

