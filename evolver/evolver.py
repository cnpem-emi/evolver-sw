#!/usr/local/bin/env python3.6
import json
import os
import socket
import yaml
import time
import traceback
from consts import functions
from evolver_server import evolverServer, serialPort, redisClient
from threading import Event, Lock, Thread

# ==============================================================
# THREADS FOR EVOLVER-SW:
#
# --- SerialThread (from evolver_server.py):
#     Dealing with serial RS485 communication and data exchange
#
# --- RedisThread (from evolver_server.py):
#     Redis local client/database for variable mirroring
#
# --- SocketServer:
#     Communication with hardware access. Based on request-reply messages
#
# --- BroadcastServer:
#     Unidirectional. Send data to client every time a new data set is available
#
# --- Main loop/thread:
#     Infinite while-loop, where broadcast/reading routine is handled
#
# ==============================================================


# ==============================================================
# Configuration file
conf = {}
CONF_FILENAME = "config/conf.yml"
with open(CONF_FILENAME, "r") as ymlfile:
    conf = yaml.load(ymlfile, Loader=yaml.FullLoader)

OD_CAL_FILE = "calibrations/od_cal.json"
TEMP_CAL_FILE = "calibrations/temp_cal.json"

# ==============================================================
# Server and TCP port
eServer = None
socketPort = 6001


# ==============================================================
# Locking object, for threads sync (similar to mutex)
lock = Lock()
broadcast_event = Event()
broadcast_data = {}


def socketServer():
    """
    Server side for evolver communication. Based on request-reply messages.

    REQUEST:
    data[0] = COMMAND_CODE (see consts.py)
    data[1:-2] = data dumped into bytes
    data[-2:] = b'\r\n' end of message

    REPLY:
    Similar structure.
    """
    while True:
        try:
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _sock.bind(("", socketPort))
            _sock.listen(1)

            while True:
                connection, client_address = _sock.accept()
                try:
                    while True:
                        msg = connection.recv(1024)
                        if msg:
                            commands = msg.split(b"\r\n")
                            for data in commands:
                                if data:
                                    # ==============================================================
                                    # command(data: dict) -> dict
                                    if data[0] == functions["command"]["id"]:
                                        info = json.loads(data[1:])
                                        info = eServer.command(info)
                                    #                                        connection.sendall(bytes(json.dumps(info), 'UTF-8') + b'\r\n')

                                    # ==============================================================
                                    # getlastcommands() -> dict
                                    elif data[0] == functions["getlastcommands"]["id"]:
                                        info = eServer.getlastcommands()
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # getcalibrationnames() -> list
                                    elif (
                                        data[0]
                                        == functions["getcalibrationnames"]["id"]
                                    ):
                                        info = eServer.getcalibrationnames()
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # getfitnames() -> list
                                    elif data[0] == functions["getfitnames"]["id"]:
                                        info = eServer.getfitnames()
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # getcalibration(data: dict) -> dict
                                    elif data[0] == functions["getcalibration"]["id"]:
                                        info = json.loads(data[1:])
                                        info = eServer.getcalibration(info)
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # setrawcalibration(data: dict) -> str
                                    elif (
                                        data[0] == functions["setrawcalibration"]["id"]
                                    ):
                                        info = json.loads(data[1:])
                                        info = eServer.setrawcalibration(info)
                                        connection.sendall(
                                            bytes(info, "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # setfitcalibrations(data: dict)
                                    elif (
                                        data[0] == functions["setfitcalibrations"]["id"]
                                    ):
                                        info = json.loads(data[1:])
                                        eServer.setfitcalibrations(info)

                                    # ==============================================================
                                    # setactiveodcal(data: dict) -> list
                                    elif data[0] == functions["setactiveodcal"]["id"]:
                                        info = json.loads(data[1:])
                                        info = eServer.setactiveodcal(info)
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # getactivecal() -> list
                                    elif data[0] == functions["getactivecal"]["id"]:
                                        print("Get active calibration...")
                                        info = eServer.getactivecal()
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # getdevicename() -> dict
                                    elif data[0] == functions["getdevicename"]["id"]:
                                        info = eServer.getdevicename()
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # setdevicename(data: dict) -> dict
                                    elif data[0] == functions["setdevicename"]["id"]:
                                        info = json.loads(data[1:])
                                        info = eServer.setdevicename(info)
                                        connection.sendall(
                                            bytes(json.dumps(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # run commands() --> dict
                                    elif data[0] == functions["run_commands"]["id"]:
                                        with lock:
                                            info = eServer.run_commands()
                                            connection.sendall(
                                                bytes(json.dumps(info), "UTF-8")
                                                + b"\r\n"
                                            )

                                    # ==============================================================
                                    # Get num commands() --> int
                                    elif data[0] == functions["get_num_commands"]["id"]:
                                        info = eServer.get_num_commands()
                                        connection.sendall(
                                            bytes(str(info), "UTF-8") + b"\r\n"
                                        )

                                    # ==============================================================
                                    # sub_command(list, dict) --> None
                                    elif data[0] == functions["sub_command"]["id"]:
                                        with lock:
                                            info = json.loads(data[1:])
                                            eServer.sub_command(info, conf)

                        else:
                            break
                except Exception:
                    # logger.exception('Connection Error !')
                    traceback.print_exc()
                finally:
                    connection.close()
        finally:
            _sock.close()


def broadcastServer():
    """
    Listen to a socket connection (dedicated port!) and send values once broadcast data/event is ready
    """
    while True:
        try:
            _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _sock.bind(("", socketPort + 1000))
            _sock.listen(1)

            while True:
                connection, client_address = _sock.accept()
                try:
                    while True:
                        broadcast_event.wait()
                        # ==============================================================
                        # command(data: dict) -> dict
                        # print('BROADCAST_DATA', broadcast_data)
                        connection.sendall(bytes(json.dumps(broadcast_data), "UTF-8"))
                        broadcast_event.clear()

                except Exception:
                    # logger.exception('Connection Error !')
                    traceback.print_exc()
                finally:
                    connection.close()
        finally:
            _sock.close()


if __name__ == "__main__":
    # need to get this unity IP
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    conf["evolver_ip"] = s.getsockname()[0]
    s.close()

    # Set up data broadcasting
    last_time = time.time()
    running = False
    broadcast_event.clear()

    # NEW THREAD
    # Set up the hardware server - Serial communication with hardware
    eServer = evolverServer(conf)
    s = serialPort(conf)
    s.run()

    # NEW THREAD
    # Redis local client/database for variable mirror
    redis = redisClient(conf, OD_CAL_FILE, TEMP_CAL_FILE)
    redis.run()

    # NEW THREAD
    # SocketServer, communicating with client (USUAL CLIENT: DPU)
    sServer = Thread(target=socketServer)
    sServer.start()

    # NEW THREAD
    # BroadcastServer, broadcasting data to client (USUAL CLIENT: DPU)
    bServer = Thread(target=broadcastServer)
    bServer.start()

    while True:
        """
        *** Infinite loop ***
        Request commands to be execute (if there are commands in queue) or
        request broadcast update if broadcast_timing is reached
        """
        current_time = time.time()
        commands_in_queue = eServer.get_num_commands() > 0

        if (
            current_time - last_time > conf["broadcast_timing"] or commands_in_queue
        ):  # and not running:
            try:
                broadcast_data = eServer.broadcast(commands_in_queue)
            except:
                pass

            if current_time - last_time > conf["broadcast_timing"]:
                last_time = current_time
                broadcast_event.set()

        time.sleep(1)
