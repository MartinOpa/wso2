import time
import os
import stomp
import json
from datetime import datetime
from enum import Enum
import csv

class MessageProcessor(stomp.ConnectionListener):

    class MessageProcessorType(Enum):
        RECEIVER = 0
        SENDER = 1

    def __init__(self, message_processor_type):

        self.message_processor_type = message_processor_type
        self.host = os.environ.get('ACTIVEMQ_HOST', '127.0.0.1')
        self.port = os.environ.get('ACTIVEMQ_PORT', 61623)
        self.queue = os.environ.get('ACTIVEMQ_QUEUE', 'csv_messages')
        self.username = os.environ.get('ACTIVEMQ_USERNAME')
        self.password = os.environ.get('ACTIVEMQ_PASSWORD')
        self.result_path = os.environ.get('CSV_PATH', './result_data/')
        self.conn = None
        self.connected = False

    def get_actor_list(self, data):

        actor_list = []

        if isinstance(data, dict):
            for key, value in data.items():
                if key == 'list':
                    actor_list.extend(value if isinstance(value, list) else [value])
                else:
                    actor_list.extend(self.get_actor_list(value))

        elif isinstance(data, list):
            for item in data:
                actor_list.extend(self.get_actor_list(item))

        return actor_list

    def on_message(self, frame):

        if self.message_processor_type != self.MessageProcessorType.RECEIVER:
            return

        try:

            message = json.loads(frame.body)
            actors = self.get_actor_list(message)

            if actors:

                for actor in actors:
                    try:
                        address = actor.pop('address')
                        for key, value in address.items():
                            actor[key] = value
                    except KeyError:
                        continue

                keys = list(set([key for actor in actors for key in actor.keys()]))

                os.makedirs(self.result_path, exist_ok=True)

                timestamp = datetime.now().strftime('%Y_%m_%d-%H:%M:%S:%f')
                filename = f'OutputFile_{timestamp}.csv'
                filepath = os.path.join(self.result_path, filename)

                with open(filepath, 'w') as file:
                    try:
                        writer = csv.DictWriter(file, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(actors)
                    except Exception as ex:
                        print(f'Error processing message: {ex}')

            else:

                # no data
                pass

        except Exception as ex:
            print(f'Error processing message: {ex}')

    def on_error(self, frame):
        print(f'Error: {frame.body}')

    def on_disconnected(self):
        self.connected = False

    def send(self, message):

        if self.message_processor_type != self.MessageProcessorType.SENDER:
            return

        try:

            if not self.connected:
                self.connect()

            self.conn.send(
                destination=self.queue,
                body=message,
                headers={'persistent': 'true'}
            )

        except Exception as e:
            print(f"Error sending message: {e}")
            self.connected = False
            raise

    def disconnect(self):

        if self.conn:
            self.conn.disconnect()
        self.connected = False

    def connect(self):

        self.conn = stomp.Connection([(self.host, self.port)])
        self.conn.set_listener('', self)
        self.conn.connect(self.username, self.password, wait=True)

        if self.message_processor_type == self.MessageProcessorType.RECEIVER:
            self.conn.subscribe(destination=self.queue, id=1, ack='auto')

        self.connected = True
        print(f'Connected to ActiveMQ at {self.host}:{self.port}, listening on {self.queue}')

    def run(self):

        while True:

            try:

                if not self.connected:
                    self.connect()
                time.sleep(5)

            except Exception as ex:

                print(ex)
                self.disconnect()
                time.sleep(5)