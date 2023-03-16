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

# As long as the listen method is not called, the server could be used as a client
class RPCServer(Server):
    def __init__(self, handler, address: tuple[str, int], client_address: tuple[str, int] = ('localhost', 8000)):
        self.handler = handler
        self.server = SimpleXMLRPCServer(address)
        self.set_client(client_address)

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

    @staticmethod
    def call_by_address(address: tuple[str, int], method, *args):
        client = ServerProxy(f'http://{address[0]}:{address[1]}')
        return getattr(client, method)(*args)