#!/usr/bin/env python3
import sys, logging, time, json
from configparser import ConfigParser
from pprint import pprint
import pika # RabbitMQ
#import ipgetter

class Messages():

    def __init__(self, type: str, config_ini: str, exchange_name: str, queue_name: str):
        """
            :param type: type of messaging - "jobs" or "results"
            :param config: ini-file
            :exchange_name: str
            :queue_name: name of queue, str type
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
            self.message = ""
            #self.routing_key = routing_key
            #self.queue_results = self.config['rabbitmq']['queue_results']
            #self.results_routing_key = self.config['rabbitmq']['results_routing_key']

            cred = pika.credentials.PlainCredentials(username=self.user_name, password=self.password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=cred))
            self.channel = self.connection.channel()

            # defining type if messaging
            self.exchange_type = "fanout" if type=="results" else "topic"
            self.channel.exchange_declare(exchange=exchange_name, exchange_type=self.exchange_type, 
                                            durable=False, passive=False, auto_delete=False)
            self.queue = self.channel.queue_declare(queue=queue_name)            
            
        except Exception as e:
            print(e)
            sys.exit()


    def send(self, message, routing_key=""):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                content_type="application/json", #"text/plain",
                content_encoding="gzip",
                delivery_mode=2,  # 1 - fire and forget, 2 - persistent
                # expiration='9000' # 9 seconds
                #expiration='2592000000' # 30 days
            )
        )


    def receive(self, callbacks=[]):
        """
        Receives messages and takes callback function as optional parameter.
        Callback function must implement resending and takes only 1 parameter - message content to resend
        """
        #self.channel.queue_bind(exchange=self.exchange_results, queue=self.queue.method.queue)
        self.channel.queue_bind(exchange=self.exchange, 
                                queue=self.queue_name,
                                # routing_key=self.routing_key,
                                # routing_key=routing_key, 
                                )

        def _callback(channel, method, properties, body):
            msg = body.decode('utf8')
            self.message = msg
            if callbacks != []:
                market, results = callbacks

                payload = json.loads(msg)
                exchange = payload['exchange']
                pair = payload['pair']

                print(f"Fetching {exchange}: {pair} ... ", end='')

                try:
                    histories = market.fetch_trades(exchange=exchange, pair=pair)
                    histories = [x.pop('info') for x in histories] # delete all infos
                    histories = {'exchange': exchange,
                                'pair': pair,
                                'histories': histories
                                }
                    last_ts = histories['histories'][-1]['timestamp']
                    last_price = histories['histories'][-1]['price']
                    results.send(json.dumps(histories), "history_results")
                    print(f"SUCCESS. {last_ts} Last price = {last_price}")
                except Exception as e:
                    print(f"FAILED.\n\n{e}\n")
            else:
                print(f"{msg} (queue={self.queue_name})")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            #results.channel.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(_callback, queue=self.queue_name, no_ack=False)
        self.channel.start_consuming()


    def __del__(self):
        self.connection.close()
        print(f"Connection for {self.exchange_type} closed.")


if __name__ == "__main__":
    print("This module is not intended for direct use")