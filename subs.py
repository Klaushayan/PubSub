from __future__ import annotations
from dataclasses import dataclass
from typing import Callable

@dataclass
class Message:
    id: int
    topic: str
    data: str


@dataclass
class Subscriber:
    id: int
    address: str
    last_message_id: int = 0
    notify: Callable[[Message], None] = lambda message: None

    def register(self, notify: Callable[[Message], None]) -> None:
        self.notify = notify


class SubscriberList:
    def __init__(self):
        self.subscribers = []
        self.next_id = 0

    def add(self, subscriber: Subscriber) -> SubscriberList:
        self.subscribers.append(subscriber)
        return self

    def remove(self, subscriber: Subscriber | int) -> SubscriberList:
        if isinstance(subscriber, Subscriber):
            self.subscribers.remove(subscriber)
        else:
            self._remove_by_id(subscriber)
        return self

    def create_subscriber(self, address: str, add: bool = True) -> Subscriber:
        subscriber = Subscriber(self._get_next_id(), address)
        if add:
            self.add(subscriber)
        return subscriber

    def _remove_by_id(self, id: int) -> SubscriberList:
        self.subscribers = [s for s in self.subscribers if s.id != id]
        return self

    def _get_next_id(self) -> int:
        self.next_id += 1
        return self.next_id

    def __len__(self) -> int:
        return len(self.subscribers)

    def __iter__(self):
        return iter(self.subscribers)

    def __getitem__(self, index: int) -> Subscriber:
        return self.subscribers[index]

    def __setitem__(self, index: int, subscriber: Subscriber) -> None:
        self.subscribers[index] = subscriber

    def __delitem__(self, index: int) -> None:
        del self.subscribers[index]

    def __contains__(self, subscriber: Subscriber | int) -> bool:
        if isinstance(subscriber, Subscriber):
            return subscriber in self.subscribers
        else:
            return any(s.id == subscriber for s in self.subscribers)
