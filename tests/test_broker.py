from filecmp import cmp
import unittest
from broker import Broker

def make_orderer():
    order = {}

    def ordered(f):
        order[f.__name__] = len(order)
        return f

    def compare(a, b):
        return [1, -1][order[a] < order[b]]

    return ordered, compare

ordered, compare = make_orderer()
unittest.defaultTestLoader.sortTestMethodsUsing = compare

class TestBroker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.broker = Broker()

    @ordered
    def test_create_topic(self):
        topic = self.broker.create_topic('test')
        self.assertEqual(topic.name, 'test')
    @ordered
    def test_add_subscriber(self):
        subscriber = self.broker.subscribe('test', 'http://localhost:8000/test1')
        self.assertEqual(subscriber.address, 'http://localhost:8000/test1')
    @ordered
    def test_add_message(self):
        message = self.broker.publish('test', 'test message')
        self.assertEqual(message.data, 'test message')
    @ordered
    def test_get_messages(self):
        subscriber = self.broker.get_topic('test').subscribers[0]
        messages = self.broker.get_messages('test', subscriber)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].data, 'test message')
    @ordered
    def test_add_subscriber_to_non_existing_topic(self):
        with self.assertRaises(ValueError):
            self.broker.subscribe('test2', 'http://localhost:8000/test2')
    @ordered
    def test_add_message_to_non_existing_topic(self):
        with self.assertRaises(KeyError):
            self.broker.publish('test2', 'test message')
    @ordered
    def test_get_messages_from_non_existing_topic(self):
        with self.assertRaises(KeyError):
            self.broker.get_messages('test2', self.broker.subscribe('test', 'http://localhost:8000/test2'))
    @ordered
    def test_add_ten_messages(self):
        for i in range(10):
            self.broker.publish('test', f'test message {i}')
        self.assertEqual(len(self.broker.get_topic('test').messages), 11)
    @ordered
    def test_check_if_subscriber_has_ten_messages(self):
        subscriber = self.broker.get_topic('test').subscribers[0]
        messages = self.broker.get_messages('test', subscriber)
        self.assertEqual(len(messages), 10)
    @ordered
    def test_check_new_subscriber_has_zero_messages(self):
        subscriber = self.broker.subscribe('test', 'http://localhost:8000/test3')
        messages = self.broker.get_messages('test', subscriber)
        self.assertEqual(len(messages), 0)

    @ordered
    def test_unsubscribe(self):
        l = len(self.broker.get_topic('test').subscribers)
        subscriber = self.broker.get_topic('test').subscribers[0]
        self.broker.unsubscribe('test', subscriber)
        self.assertEqual(len(self.broker.get_topic('test').subscribers), l - 1)