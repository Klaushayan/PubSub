from abc import ABC, abstractmethod
import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

class Server(ABC):
    @abstractmethod
    def listen(self, address):
        pass

    @abstractmethod
    def close(self):
        pass

class RPCServer(Server):
    def __init__(self, handler, address: tuple[str, int]):
        self.handler = handler
        self.server = SimpleXMLRPCServer(address)
        self.client = ServerProxy('http://localhost:8000') # Dummy client

    def listen(self, address = None):
        if address is not None:
            self.server = SimpleXMLRPCServer(address)
        self.server.register_instance(self.handler)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join()

    def call(self, method, *args):
        return getattr(self.client, method)(*args)

    def set_client(self, address: tuple[str, int]):
        self.client = ServerProxy(f'http://{address[0]}:{address[1]}')