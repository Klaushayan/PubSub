

from broker import Broker
from server import RPCServer


if __name__ == "__main__":
    rpc_server = RPCServer(Broker(), ('localhost', 8000))
    rpc_server.listen()

