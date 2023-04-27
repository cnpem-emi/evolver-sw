#!/usr/local/bin/env python3.6
import yaml
import time
import socket
from evolver_server import evolverServer, serialPort, redisClient
import os
from threading import Thread

conf = {}
CONF_FILENAME = 'conf.yml'

with open(CONF_FILENAME, 'r') as ymlfile:
    conf = yaml.load(ymlfile)

es = evolverServer(conf)
es.sub_command([{"param":"stir", "value":['8']*16, "type":"immediate_command_char"}], conf)

s=serialPort(conf)
s.run()

redis = redisClient(conf)
redis.run()




if __name__ == '__main__':
    
    with open(os.path.realpath(os.path.join(os.getcwd(),os.path.dirname(__file__), CONF_FILENAME)), 'r') as ymlfile:
        conf = yaml.load(ymlfile)
    
    # need to get IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    conf['evolver_ip'] = s.connect(("8.8.8.8", 80)).getsockname()[0]
    s.close()

    # Set up the server
    eServer = evolverServer(conf)


    # Set up data broadcasting
    last_time = time.time()

    while True:
        current_time = time.time()
        commands_in_queue = eServer.get_num_commands() == 0

        if (current_time - last_time > conf['broadcast_timing'] or commands_in_queue):
            if current_time - last_time > conf['broadcast_timing']:
                last_time = current_time
            try:
                for param in conf['experimental_params'].keys():
                    es.sub_command([{"param": param, "value":['-']*16, "type": "reading_command_char"}], conf)
                
                replies = es.run_commands()

            except:
                pass
