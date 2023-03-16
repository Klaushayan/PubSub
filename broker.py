from abc import ABC, abstractmethod
from subs import Message, Subscriber
from topic import Topic


class BaseBroker(ABC):
    @abstractmethod
    def create_topic(self, topic_name: str) -> Topic:
        pass
    @abstractmethod
    def subscribe(self, topic_name: str, subscriber_address: str) -> Subscriber:
        pass

    @abstractmethod
    def unsubscribe(self, topic_name: str, subscriber: int | Subscriber):
        pass

    @abstractmethod
    def publish(self, topic_name: str, message: Message | str):
        pass


# basically, an in-memory broker
class Broker(BaseBroker):
    def __init__(self):
        self.topics: dict[str, Topic] = {}

    def create_topic(self, topic_name: str) -> Topic:
        try:
            self.validate_topic_name(topic_name)
            if self.topic_exists(topic_name):
                raise ValueError(f"Topic {topic_name} already exists")
            topic = Topic(topic_name)
            self.topics[topic_name] = topic
            return topic
        except ValueError as e:
            raise ValueError(f"Invalid topic name: {e}")

    def get_topic(self, topic_name: str) -> Topic:
        return self.topics[topic_name]

    def get_topic_names(self) -> tuple[str]:
        return tuple(self.topics.keys())

    def topic_exists(self, topic_name: str) -> bool:
        return topic_name in self.topics

    def subscribe(self, topic_name: str, subscriber_address: str) -> Subscriber:
        try:
            self.validate_subscriber_address(subscriber_address)
            topic = self.get_topic(topic_name)
            return topic.add_subscriber(subscriber_address)
        except KeyError:
            topic = self.create_topic(topic_name)
            return topic.add_subscriber(subscriber_address)
        except ValueError as e:
            raise ValueError(f"Invalid subscriber address: {e}")

    def unsubscribe(self, topic_name: str, subscriber: int | Subscriber):
        topic = self.get_topic(topic_name)
        topic.remove_subscriber(subscriber)

    def publish(self, topic_name: str, message: Message | str):
        topic = self.get_topic(topic_name)
        return topic.add_message(message)

    def get_messages(self, topic_name: str, subscriber: Subscriber) -> list[Message]:
        topic = self.get_topic(topic_name)
        return topic.get_messages(subscriber)

    def has_messages(self, topic_name: str, subscriber: Subscriber) -> bool:
        topic = self.get_topic(topic_name)
        return topic.has_new_messages(subscriber)

    def has_any_messages(self, subscriber: Subscriber) -> bool:
        for topic_name in self.get_topic_names():
            if self.has_messages(topic_name, subscriber):
                return True
        return False

    @staticmethod
    def validate_topic_name(topic_name: str):
        if not topic_name.isalnum():
            raise ValueError(f"Topic name must be alphanumeric, got {topic_name}")

    @staticmethod
    def validate_subscriber_address(subscriber_address: str):
        if not subscriber_address.startswith("http"):
            raise ValueError(
                f"Subscriber address must start with http, got {subscriber_address}"
            )
