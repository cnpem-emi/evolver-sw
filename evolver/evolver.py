#!/usr/local/bin/env python3.6
import json
import os
import yaml
import time
import socket
from consts import commands
from evolver_server import evolverServer, serialPort, redisClient
from threading import Thread

conf = {}
CONF_FILENAME = 'conf.yml'

with open(CONF_FILENAME, 'r') as ymlfile:
    conf = yaml.load(ymlfile, Loader=yaml.FullLoader)

es = evolverServer(conf); s=serialPort(conf); s.run(); redis = redisClient(conf); redis.run()


es.sub_command([{"param":"stir", "value":['8']*16, "type":"immediate_command_char"}], conf)
es.run_commands()


eServer = None


def socketServer(port):
    while True:
        try:
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _sock.bind("", port)
            _sock.listen(1)

            while True:
                connection, client_address = _sock.accept()
                try:
                    while True:
                        msg = connection.recv(1024)
                        if msg:
                            commands = msg.split(b'\r\n')
                            for data in commands:
                                if data:
                                    #==============================================================
                                    # run commands() --> dict
                                    if (data[0] == commands["run_commands"]["id"]):
                                        info = eServer.run_commands()
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')
                                    #==============================================================
                                    #==============================================================
                                    # Get num commands() --> int
                                    elif (data[0] == commands["get_num_commands"]["id"]):
                                        info = eServer.get_num_commands()
                                        connection.sendall(bytes(str(info), 'UTF-8') + b'\r\n')
                                    #==============================================================
                                    #==============================================================
                                    # sub_command(list, dict) --> None
                                    elif (data[0] == commands["sub_command"]["id"]):
                                        info = json.loads(data[1:])
                                        eServer.sub_command(info, conf)
                                    #==============================================================

                        else:
                            break
                except:
                    #logger.exception('Connection Error !')
                    print('Error in thread 1! (writing to list')
                finally:
                    connection.close()
        finally:
            _sock.close()
    



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
