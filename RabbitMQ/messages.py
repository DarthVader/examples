#!/usr/bin/env python3
import sys, logging, time
from configparser import ConfigParser
from pprint import pprint
import pika # RabbitMQ
#import ipgetter

class Results():

    def __init__(self, config_ini: str, exchange_name: str, queue_name: str):
        """
            :param config: ini-file
            :exchange_name: str
            :queue_name: name of queue, str type
            :routing_key: str
        """   
        try:
            self.config = ConfigParser()
            self.config.read(config_ini)
            self.host = self.config['rabbitmq']['server_address']
            self.port = self.config['rabbitmq']['port']
            self.user_name = self.config['rabbitmq']['user_name']
            self.password = self.config['rabbitmq']['password']

            self.exchange = exchange_name #self.config['rabbitmq']['exchange_results']
            self.queue_name = queue_name
            #self.routing_key = routing_key
            #self.queue_results = self.config['rabbitmq']['queue_results']
            #self.results_routing_key = self.config['rabbitmq']['results_routing_key']

            cred = pika.credentials.PlainCredentials(username=self.user_name, password=self.password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=cred))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type="fanout", durable=False, passive=False, auto_delete=False)
            #self.queue = self.channel.queue_declare(queue=queue_name, exclusive=True)
            #self.queue = self.channel.queue_declare(queue=queue_name, durable=True) # http://www.rabbitmq.com/queues.html  (durable, exclusive, auto_delete..)
            self.queue = self.channel.queue_declare(queue=queue_name)
            
            #self.timer = time.time()
        except Exception as e:
            print(e)
            sys.exit()


    def send(self, message):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key="",
            body=message,
            properties=pika.BasicProperties(
                content_type="text/plain",
                content_encoding="gzip",
                delivery_mode=2,  # 1 - fire and forget, 2 - persistent
                #expiration='3000'
            )
        )
        #self.timer = time.time()

    def receive(self):
        #self.channel.queue_bind(exchange=self.exchange_results, queue=self.queue.method.queue)
        self.channel.queue_bind(exchange=self.exchange, 
                                queue=self.queue_name,
                                #routing_key=self.routing_key,
                                # routing_key=routing_key, 
                                )

        def _callback(channel, method, properties, body):
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"{body.decode('utf8')} (queue={self.queue_name})")
            #logging.info(body)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(_callback, queue=self.queue_name, no_ack=False)
        self.channel.start_consuming()

    def __del__(self):
        self.connection.close()
        print("Connection closed.")
