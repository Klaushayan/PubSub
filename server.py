from abc import ABC, abstractmethod

class Server(ABC):
    @abstractmethod
    def send(self, message: dict, client_id: str) -> bool:
        pass

    @abstractmethod
    def recv(self) -> dict:
        pass