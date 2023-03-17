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

    @abstractmethod
    def get_messages(self, topic_name: str, subscriber: Subscriber):
        pass


# basically, an in-memory broker
class Broker(BaseBroker):
    def __init__(self):
        self.topics: dict[str, Topic] = {}

    def create_topic(self, topic_name: str) -> Topic:
        """Creates a new topic.

        If the topic already exists, raises a ValueError.
        If the topic name is invalid, raises a ValueError.
        If an unexpected error occurs, raises an Exception.

        Args:
            topic_name: The name of the topic to create.

        Returns:
            The newly created Topic.
        """
        try:
            self.validate_topic_name(topic_name)
            if self.topic_exists(topic_name):
                raise ValueError(f"Topic {topic_name} already exists")
            return self._create_topic(topic_name)
        except ValueError as e:
            raise ValueError(f"Invalid topic name: {e}")
        except Exception as e:
            raise Exception(f"Could not create topic {topic_name}: {e}")

    def _create_topic(self, topic_name: str) -> Topic:
        topic = Topic(topic_name)
        self.topics[topic_name] = topic
        return topic

    def get_topic(self, topic_name: str) -> Topic:
        """
        Returns the topic with the given topic_name.
        If the topic does not exist, it will raise a KeyError.
        """
        return self.topics[topic_name]

    def get_topic_names(self) -> tuple[str]:
        return tuple(self.topics.keys())

    def topic_exists(self, topic_name: str) -> bool:
        return topic_name in self.topics

    def subscribe(self, topic_name: str, subscriber_address: str) -> Subscriber:
        """ Adds a subscriber to a topic.

        Args:
            topic_name: The name of the topic.
            subscriber_address: The address of the new subscriber.

        Returns:
            The new subscriber.

        Raises:
            KeyError: The topic does not exist.
            ValueError: The subscriber address is invalid.
        """
        try:
            self.validate_subscriber_address(subscriber_address)
            topic = self.get_topic(topic_name)
        except KeyError:
            topic = self.create_topic(topic_name)
        except ValueError as e:
            raise ValueError(f"Invalid subscriber address: {e}")
        return topic.add_subscriber(subscriber_address)

    def unsubscribe(self, topic_name: str, subscriber: int | Subscriber):
        """
        Unsubscribe a subscriber from a topic
        """
        topic = self.get_topic(topic_name)
        topic.remove_subscriber(subscriber)

    def publish(self, topic_name: str, message: Message | str):
        # Get the topic that the message will be published to.
        topic = self.get_topic(topic_name)
        # Add the message to the topic.
        return topic.add_message(message)

    def get_messages(self, topic_name: str, subscriber: Subscriber, update: bool = False) -> list[Message]:
        """Get all messages in a topic that are not yet read by a subscriber.
        The update parameter determines whether the subscriber's last read message ID will be updated.
        You could later manually update the subscriber's last read message ID by calling topic.update_subscriber(subscriber, message_id).
        Upon confirmation that the subscriber has recieved the messages.

        Note: You could access a topic's object directly by calling broker.get_topic(topic_name).

        Args:
            topic_name: The name of the topic.
            subscriber: The subscriber.
            update: Whether to update the subscriber's last read message ID.

        Returns:
            A list of messages.
        """
        topic = self.get_topic(topic_name)
        messages = topic.get_messages(subscriber)
        if update and messages:
            topic.update_subscriber(subscriber, messages[-1].id)
        return messages

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
