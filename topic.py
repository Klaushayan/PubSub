from dataclasses import dataclass
from subs import Message, Subscriber, SubscriberList


class Topic(object):
    def __init__(self, name: str):
        self.name: str = name
        self.subscribers: SubscriberList = SubscriberList()
        self.messages: list[Message] = []
        self.next_message_id: int = 0

    def add_subscriber(self, subscriber_address: str) -> Subscriber:
        subscriber = self.subscribers.create_subscriber(subscriber_address)
        subscriber.last_message_id = self.next_message_id
        return subscriber

    def remove_subscriber(self, subscriber: int | Subscriber):
        return self.subscribers.remove(subscriber)

    def add_message(self, message: Message | str):
        if isinstance(message, str):
            message = Message(self._get_next_message_id(), self.name, message)
        self.messages.append(message)
        return message

    def get_messages(self, subscriber: Subscriber):
        messages = self.messages[subscriber.last_message_id:]
        subscriber.last_message_id = self.next_message_id
        return messages

    def get_subscriber(self, subscriber_id: int) -> Subscriber:
        return self.subscribers[subscriber_id]

    def get_subscriber_by_address(self, address: str) -> Subscriber:
        for subscriber in self.subscribers:
            if subscriber.address == address:
                return subscriber
        raise ValueError(f'No subscriber with address {address}')

    # this is to be used if a TTL functionality is implemented
    def reset_messages(self):
        self.messages = []
        self.next_message_id = 0
        self._reset_subscribers_last_message_id()

    def _reset_subscribers_last_message_id(self):
        for subscriber in self.subscribers:
            subscriber.last_message_id = 0

    @property
    def has_subscribers(self):
        return len(self.subscribers) > 0

    @property
    def has_messages(self):
        return len(self.messages) > 0

    @property
    def subscriber_count(self):
        return len(self.subscribers)

    @property
    def latest_message(self):
        return self.messages[-1]

    def _get_next_message_id(self) -> int:
        self.next_message_id += 1
        return self.next_message_id

    def __repr__(self):
        return self.name