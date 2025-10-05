from flask import Flask
from flask.cli import load_dotenv
import os
import argparse
import time
import threading

from wso2.message_processor import MessageProcessor
from wso2 import xml_to_queue

class LaunchableThread():

    def __init__(self, target_func):
        self.target_func = target_func
        self.enabled = True
        self.thread = None

    def run(self):
        while self.enabled:
            try:
                self.target_func()
            except Exception as ex:
                time.sleep(1)
                continue

    def start(self):
        self.enabled = True
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.enabled = False
        if self.thread:
            self.thread.join()

def get_app(env_path=None):

    if env_path is not None:
        env_path = os.path.abspath(env_path)

    if env_path is None or not os.path.exists(env_path):
        env_path = os.path.join(os.getcwd(), '.env')

    load_dotenv(env_path)

    app = Flask('WSO2 Server')
    app.config.from_prefixed_env()

    return app

def start_flask():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--env',
        type=str,
        default=None,
        help='path to .env',
    )
    args = parser.parse_args()
    app = get_app(args.env)

    host = os.environ.get('HOST_IP', '127.0.0.1')
    port = os.environ.get('HOST_PORT', 8000)
    app.register_blueprint(xml_to_queue.bp)
    app.run(host=host, port=port)

def start_consumer():

    consumer = MessageProcessor(message_processor_type=MessageProcessor.MessageProcessorType.RECEIVER)
    consumer.run()

def start():

    flask_thread = LaunchableThread(start_flask)
    consumer_thread = LaunchableThread(start_consumer)

    try:

        flask_thread.start()
        consumer_thread.start()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        flask_thread.stop()
        consumer_thread.stop()

start()