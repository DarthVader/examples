#!/usr/bin/env python3
import sys, logging, time
from configparser import ConfigParser
from pprint import pprint
import pika # RabbitMQ
import ipgetter

class Results():
    def __init__(self, config_ini: str, queue_name: str):
        """
            :param config: ini-file
            :queue_name: name of queue, str type
        """   
        try:
            self.config = ConfigParser()
            self.config.read(config_ini)
            self.host = self.config['rabbitmq']['server_address']
            self.port = self.config['rabbitmq']['port']
            self.user_name = self.config['rabbitmq']['user_name']
            self.password = self.config['rabbitmq']['password']

            self.exchange_results = self.config['rabbitmq']['exchange_results']
            self.queue_results = self.config['rabbitmq']['queue_results']

            cred = pika.credentials.PlainCredentials(username=self.user_name, password=self.password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=cred))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_results, exchange_type='fanout')
            #self.queue = self.channel.queue_declare(queue=queue_name, exclusive=True)
            self.queue = self.channel.queue_declare(queue=queue_name, durable=True) # http://www.rabbitmq.com/queues.html  (durable, exclusive, auto_delete..)
            
            #self.timer = time.time()
        except Exception as e:
            print(e)
            sys.exit()


    def send(self, message):
        self.channel.basic_publish(
            exchange=self.exchange_results, 
            routing_key="",
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # 1 - fire and forget, 2 - persistent
                #expiration='3000'
            )
        )
        #self.timer = time.time()

    def _callback(self, channel, method, properties, body):
        print(body.decode('utf8'))
        #logging.info(body)
        #channel.basic_ack(delivery_tag = method.delivery_tag)

    def receive(self):
        #self.channel.queue_bind(exchange=self.exchange_results, queue=self.queue.method.queue)
        self.channel.queue_bind(exchange=self.exchange_results, queue=self.queue_results)
        #self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self._callback, queue=self.queue_results, no_ack=True)
        self.channel.start_consuming()

    def __del__(self):
        self.connection.close()
        print("Connection closed.")
