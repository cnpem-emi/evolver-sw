#!/usr/local/bin/env python3.6
import json
import socket
import yaml
import time
import traceback
import asyncio
from multi_server import MultiServer
from consts import functions
from evolver_server import evolverServer, serialPort, redisClient
from threading import Lock, Thread


# ==============================================================
# Configuration file
conf = {}
CONF_FILENAME = 'conf.yml'
with open(CONF_FILENAME, 'r') as ymlfile:
    conf = yaml.load(ymlfile, Loader=yaml.FullLoader)


# ==============================================================
# Server
eServer = None
socketioPort = 6001



if __name__ == '__main__':

    # need to get IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    conf['evolver_ip'] = s.getsockname()[0]
    s.close()

    # Set up the server
    eServer = evolverServer(conf)
    s=serialPort(conf)
    s.run()
    
    redis = redisClient(conf)
    redis.run()

    # Set up the server
    server_loop = asyncio.new_event_loop()
    ms = MultiServer(loop=server_loop)
    app1 = ms.add_app(port = conf['port'])
    eServer.attach(app1)
    ms.run_all()



    # Set up data broadcasting
    broadcastLoop = asyncio.new_event_loop()
    last_time = None
    running = False

    while True:
        current_time = time.time()
        commands_in_queue = eServer.get_num_commands() > 0

        if (last_time is None or current_time - last_time > conf['broadcast_timing'] or commands_in_queue) and not running:
            if last_time is None or current_time - last_time > conf['broadcast_timing']:
                last_time = current_time
            try:
                running = True
                broadcastLoop.run_until_complete(eServer.broadcast(commands_in_queue))
                running = False
            except:
                pass
        time.sleep(5)