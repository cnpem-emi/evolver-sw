#!/usr/local/bin/env python3.6
import json
import os
import socket
import yaml
import time
import traceback
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
# Server and TCP port
eServer = None
socketPort = 6000


# ==============================================================
# Locking object, for threads sync
lock = Lock()




def socketServer(port):
    '''
        
    '''
    while True:
        try:
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _sock.bind(("", port))
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
                                    # ==============================================================
                                    # command(data: dict) -> dict
                                    if (data[0] == functions["command"]["id"]):
                                        info = json.loads(data[1:])
                                        info = eServer.command(info)
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getlastcommands() -> dict
                                    elif (data[0] == functions["getlastcommands"]["id"]):
                                        info = eServer.getlastcommands()
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getcalibrationnames() -> list
                                    elif (data[0] == functions["getcalibrationnames"]["id"]):
                                        info = eServer.getcalibrationnames()
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getfitnames() -> list
                                    elif (data[0] == functions["getfitnames"]["id"]):
                                        info = eServer.getfitnames()
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getcalibration(data: dict) -> dict
                                    elif (data[0] == functions["getcalibration"]["id"]):
                                        info = json.loads(data[1:])
                                        info = eServer.getcalibration(info)
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # setrawcalibration(data: dict) -> str
                                    elif (data[0] == functions["setrawcalibration"]["id"]):
                                        info = json.loads(data[1:])
                                        info = eServer.setrawcalibration(info)
                                        connection.sendall(bytes(info, 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # setfitcalibrations(data: dict)
                                    elif (data[0] == functions["setfitcalibrations"]["id"]):
                                        info = json.loads(data[1:])
                                        eServer.setfitcalibrations(info)

                                    # ==============================================================
                                    # setactiveodcal(data: dict) -> list
                                    elif (data[0] == functions["setactiveodcal"]["id"]):
                                        info = json.loads(data[1:])
                                        info = eServer.setactiveodcal(info)
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getactivecal() -> list
                                    elif (data[0] == functions["getactivecal"]["id"]):
                                        info = eServer.getactivecal()
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getdevicename() -> dict
                                    elif (data[0] == functions["getdevicename"]["id"]):
                                        info = eServer.getdevicename()
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # setdevicename(data: dict) -> dict
                                    elif (data[0] == functions["setdevicename"]["id"]):
                                        info = json.loads(data[1:])
                                        info = eServer.setdevicename(info)
                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # run commands() --> dict
                                    elif (data[0] == functions["run_commands"]["id"]):
                                        with lock:
                                            info = eServer.run_commands()
                                            connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # Get num commands() --> int
                                    elif (data[0] == functions["get_num_commands"]["id"]):
                                        info = eServer.get_num_commands()
                                        connection.sendall(bytes(str(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # sub_command(list, dict) --> None
                                    elif (data[0] == functions["sub_command"]["id"]):
                                        with lock:
                                            info = json.loads(data[1:])
                                            eServer.sub_command(info, conf)

                        else:
                            break
                except Exception:
                    #logger.exception('Connection Error !')
                    traceback.print_exc()
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
    s=serialPort(conf)
    s.run()
    
    redis = redisClient(conf)
    redis.run()

    sServer = Thread(target=socketServer, args=(socketPort))
    sServer.start()


    # Set up data broadcasting
    last_time = time.time()

    while True:
        current_time = time.time()
        no_commands_in_queue = eServer.get_num_commands() == 0

        if (current_time - last_time > conf['broadcast_timing'] or no_commands_in_queue):
            if current_time - last_time > conf['broadcast_timing']:
                last_time = current_time
            try:
                with lock:
                    for param in conf['experimental_params'].keys():
                        eServer.sub_command([{"param": param, "value":['-']*16, "type": "reading_command_char"}], conf)
                    replies = eServer.run_commands()

            except:
                pass
