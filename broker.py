
from dataclasses import dataclass

from subs import Subscriber, SubscriberList


@dataclass
class Message:
    id: int
    topic: str
    data: str

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

class Broker:
    def __init__(self):
        self.topics: dict[str, Topic] = {}

    def create_topic(self, topic_name: str) -> Topic:
        try:
            Broker.validate_topic_name(topic_name)
            if self.topic_exists(topic_name):
                raise ValueError(f'Topic {topic_name} already exists')
            topic = Topic(topic_name)
            self.topics[topic_name] = topic
            return topic
        except ValueError as e:
            raise ValueError(f'Invalid topic name: {e}')

    def get_topic(self, topic_name: str) -> Topic:
        return self.topics[topic_name]

    def get_topic_names(self) -> tuple[str]:
        return tuple(self.topics.keys())

    def topic_exists(self, topic_name: str) -> bool:
        return topic_name in self.topics

    def subscribe(self, topic_name: str, subscriber_address: str) -> Subscriber:
        try:
            Broker.validate_subscriber_address(subscriber_address)
            topic = self.get_topic(topic_name)
            return topic.add_subscriber(subscriber_address)
        except KeyError:
            raise ValueError(f'Topic {topic_name} does not exist')
        except ValueError as e:
            raise ValueError(f'Invalid subscriber address: {e}')

    def unsubscribe(self, topic_name: str, subscriber: int | Subscriber):
        topic = self.get_topic(topic_name)
        topic.remove_subscriber(subscriber)

    def publish(self, topic_name: str, message: Message | str):
        topic = self.get_topic(topic_name)
        return topic.add_message(message)

    def get_messages(self, topic_name: str, subscriber: Subscriber) -> list[Message]:
        topic = self.get_topic(topic_name)
        return topic.get_messages(subscriber)

    @staticmethod
    def validate_topic_name(topic_name: str):
        if not topic_name.isalnum():
            raise ValueError(f'Topic name must be alphanumeric, got {topic_name}')

    @staticmethod
    def validate_subscriber_address(subscriber_address: str):
        if not subscriber_address.startswith('http'):
            raise ValueError(f'Subscriber address must start with http, got {subscriber_address}')
