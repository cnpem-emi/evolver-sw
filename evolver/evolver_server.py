import serial
import time
import json
import logging
import sys
import os
import yaml
import redis
import numpy as np
from threading import Event, Thread
from traceback import print_exc
from queue import Queue

LOCATION = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
IMMEDIATE = "immediate_command_char"
RECURRING = "recurring_command_char"
READ_ONLY = "reading_command_char"
CALIBRATIONS_FILENAME = "calibrations/calibrations.json"
CHANNEL_INDEX_PATH = "config/channel_index.json"


# ---- QUEUES
serialQueue = Queue()  # Items in queue: {"payload": bytes, "reply": boolean}
redisQueue = (
    Queue()
)  #                  payload: bytes to be sent // reply: wait for a reply?
broadcastQueue = Queue()
serialResponseQueue = Queue()

# ---- REDIS CLIENT
REDIS_INCOMING_QUEUE = "incoming_queue"
REDIS_OUTCOMING_QUEUE = "outcoming_queue"


# ---- LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-15s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
logger = logging.getLogger()


class EvolverSerialError(Exception):
    pass


class evolverServer:
    def __init__(self, config: dict):
        """ """
        self.command_queue = Queue()
        self.evolver_conf = config

    def command(self, data: dict) -> dict:
        """ """
        param = data.get("param", None)
        value = data.get("value", None)
        immediate = data.get("immediate", None)
        recurring = data.get("recurring", None)
        readonly = data.get("readonly", None)
        fields_expected_outgoing = data.get("fields_expected_outgoing", None)
        fields_expected_incoming = data.get("fields_expected_incoming", None)

        # Update the configuration for the param
        if value is not None:
            if (
                type(value) is list
                and self.evolver_conf["experimental_params"][param]["value"] is not None
            ):
                for i, v in enumerate(value):
                    if v != "NaN":
                        self.evolver_conf["experimental_params"][param]["value"][
                            i
                        ] = value[i]
            else:
                self.evolver_conf["experimental_params"][param]["value"] = value

        if recurring is not None:
            self.evolver_conf["experimental_params"][param]["recurring"] = recurring

        if fields_expected_outgoing is not None:
            self.evolver_conf["experimental_params"][param][
                "fields_expected_outgoing"
            ] = fields_expected_outgoing

        if fields_expected_incoming is not None:
            self.evolver_conf["experimental_params"][param][
                "fields_expected_incoming"
            ] = fields_expected_incoming

        # Save to config the values sent in for the parameter
        #        with open(os.path.realpath(os.path.join(os.getcwd(),os.path.dirname(__file__), evolver.CONF_FILENAME)), 'w') as ymlfile:
        #            yaml.dump(self.evolver_conf, ymlfile)

        if immediate:
            self.clear_broadcast(param)
            self.command_queue.put({"param": param, "value": value, "type": IMMEDIATE})
        elif readonly:
            self.clear_broadcast(param)
            self.command_queue.put({"param": param, "value": value, "type": READ_ONLY})

        return data

    def getlastcommands(self) -> dict:
        """ """
        return self.evolver_conf

    def getcalibrationnames(self) -> list:
        """ """
        calibration_names = []
        print("Retrieving cal names...", flush=True)
        try:
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)

                for calibration in calibrations:
                    calibration_names.append(
                        {
                            "name": calibration["name"],
                            "calibrationType": calibration["calibrationType"],
                        }
                    )

        except FileNotFoundError:
            self.print_calibration_file_error()

        return calibration_names

    def getfitnames(self) -> list:
        """
        Get a list of all fit names in the calibration file.
        """
        fit_names = []
        print("Retrieving fit names...", flush=True)
        try:
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                
                for calibration in calibrations:
                    for fit in calibration["fits"]:
                        try:
                            fit_names.append(
                                {
                                    "name": fit["name"],
                                    "calibrationType": calibration["calibrationType"],
                                }
                            )

                        except KeyError:
                            fit_names.append(
                                {
                                    "name": fit["name"],
                                    "calibrationType": calibration["calibrationtype"],
                                }
                        )

            return fit_names

        except FileNotFoundError:
            self.print_calibration_file_error()

    def getcalibration(self, data: dict) -> dict:
        """ """
        try:
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    if calibration["name"] == data["name"]:
                        return calibration

        except FileNotFoundError:
            self.print_calibration_file_error()

    def appendcal(self, data: dict) -> str:
        """ """
        
        try:
            calibrations = []
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                calibrations.append(data)
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), "w") as f:
                json.dump(calibrations, f)

            return "success"

        except FileNotFoundError:
            self.print_calibration_file_error()

    def setrawcalibration(self, data: dict) -> str:
        """ """
        try:
            calibrations = []
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)

                # First, delete existing calibration by same name if it exists
                index_to_delete = -1
                for i, calibration in enumerate(calibrations):
                    if calibration["name"] == data["name"]:
                        index_to_delete = i
                if index_to_delete >= 0:
                    del calibrations[index_to_delete]
                """
                    Add the calibration into the list. `data` should be formatted according
                    to the cal schema, containing a name, params, and raw field.
                """
                calibrations.append(data)
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), "w") as f:
                json.dump(calibrations, f)

            return "success"
        except FileNotFoundError:
            self.print_calibration_file_error()

    def setfitcalibrations(self, data: dict):
        """
        Set a fit calibration into the calibration file. data should contain a `fit` key/value
        formatted according to the cal schema `fit` object. This function will add the fit into the
        fits list for a given calibration.
        """
        try:
            calibrations = []
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    if calibration["name"] == data["name"]:
                        if calibration.get("fits", None) is not None:
                            index_to_delete = -1
                            for i, fit in enumerate(calibration["fits"]):
                                if fit["name"] == data["fit"]["name"]:
                                    index_to_delete = i
                            if index_to_delete >= 0:
                                del calibrations["fits"][index_to_delete]
                            calibration["fits"].append(data["fit"])
                        else:
                            calibration["fits"] = [].append(data["fit"])
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), "w") as f:
                json.dump(calibrations, f)
        except FileNotFoundError:
            self.print_calibration_file_error()

    def setactiveodcal(self, data: dict) -> list:
        """ """
        try:
            active_calibrations = []
            print("Time to set active cals. Data received: ")
            print(data, flush=True)
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    active = False
                    for fit in calibration["fits"]:
                        if fit["name"] in data["calibration_names"]:
                            fit["active"] = True
                            active = True
                        else:
                            fit["active"] = False
                    if active:
                        active_calibrations.append(calibration)
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), "w") as f:
                json.dump(calibrations, f)

            return active_calibrations
        except FileNotFoundError:
            self.print_calibration_file_error()

    def getactivecal(self) -> list:
        """ """
        try:
            active_calibrations = []
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    for fit in calibration["fits"]:
                        if fit["active"]:
                            active_calibrations.append(calibration)
                            break
            return active_calibrations
        except FileNotFoundError:
            self.print_calibration_file_error()

    def getdevicename(self) -> dict:
        """
        Get device info from evolver-config.json,
        which has to be in the same directory as this file.

        Returns:
        --------
        configJSON: dict
            Dictionary containing device info.
        """
        self.config_path = os.path.join(LOCATION)
        with open(os.path.join(LOCATION, self.evolver_conf["device"])) as f:
            configJSON = json.load(f)
        return configJSON

    def setdevicename(self, data: dict) -> dict:
        """ """
        self.config_path = os.path.join(LOCATION)
        print("saving device name", flush=True)
        if not os.path.isdir(self.config_path):
            os.mkdir(self.config_path)
        with open(
            os.path.join(self.config_path, self.evolver_conf["device"]), "w"
        ) as f:
            f.write(json.dumps(data))
        return data

    def print_calibration_file_error(self):
        """ """
        print("Error reading calibrations file.", flush=True)

    def clear_broadcast(self, param=None):
        """Removes broadcast commands of a specific param from queue"""
        for command in self.command_queue.queue:
            if (command["param"] == param or param is None) and command[
                "type"
            ] == RECURRING:
                self.command_queue.queue.remove(command)
                break

    def run_commands(self) -> dict:
        """ """
        data = {}
        while self.command_queue.qsize() > 0:
            command = self.command_queue.get()
            try:
                if command["param"] == "wait":
                    time.sleep(command["value"])
                    continue
                returned_data = self.serial_communication(
                    command["param"], command["value"], command["type"]
                )
                if returned_data is not None:
                    data[command["param"]] = returned_data
            except (
                TypeError,
                ValueError,
                serial.serialutil.SerialException,
                EvolverSerialError,
            ) as e:
                print_exc(file=sys.stdout)
        return data

    def serial_communication(self, param: str, value: list, comm_type: str) -> list:
        """ """

        output = []

        # Check that parameters being sent to arduino match expected values
        if comm_type == RECURRING:
            output.append(self.evolver_conf[RECURRING])
        elif comm_type == IMMEDIATE:
            output.append(self.evolver_conf[IMMEDIATE])
        elif comm_type == READ_ONLY:
            output.append(self.evolver_conf[READ_ONLY])

        if type(value) is list:
            output = output + list(map(str, value))
            for i, command_value in enumerate(output):
                if command_value == "NaN":
                    output[i] = self.evolver_conf["experimental_params"][param][
                        "value"
                    ][i - 1]
        else:
            output.append(value)

        fields_expected_outgoing = self.evolver_conf["experimental_params"][param][
            "fields_expected_outgoing"
        ]
        fields_expected_incoming = self.evolver_conf["experimental_params"][param][
            "fields_expected_incoming"
        ]

        if len(output) is not fields_expected_outgoing:
            raise EvolverSerialError(
                "Error: Number of fields outgoing for "
                + param
                + " different from expected\n\tExpected: "
                + str(fields_expected_outgoing)
                + "\n\tFound: "
                + str(len(output))
            )

        # Construct the actual string and write out on the serial buffer
        print("Updating param ", param)
        serial_output = (
            param + ",".join(output) + "," + self.evolver_conf["serial_end_outgoing"]
        )
        serial_output = serial_output.replace("nan", "--")

        serialEvent = Event()
        serialQueue.put(
            {
                "event": serialEvent,
                "payload": bytes(serial_output, "UTF-8"),
                "reply": True,
            }
        )
        serialEvent.wait()

        # Read and process the response
        response = serialResponseQueue.get(block=True)
        if type(response) == bytes:
            response = response.decode("UTF-8", errors="ignore")

        address = response[0 : len(param)]
        if address != param:
            raise EvolverSerialError(
                "Error: Response has incorrect address.\n\tExpected: "
                + param
                + "\n\tFound:"
                + address
            )
        if response.find(self.evolver_conf["serial_end_incoming"]) != len(
            response
        ) - len(self.evolver_conf["serial_end_incoming"]):
            raise EvolverSerialError(
                "Error: Response did not have valid serial communication termination string!\n\tExpected: "
                + self.evolver_conf["serial_end_incoming"]
                + "\n\tFound: "
                + response[len(response) - 3 :]
            )

        # Remove the address and ending from the response string and convert to a list
        returned_data = response[
            len(param) : len(response)
            - len(self.evolver_conf["serial_end_incoming"])
            - 1
        ].split(",")

        if len(returned_data) != fields_expected_incoming:
            raise EvolverSerialError(
                "Error: Number of fields received for "
                + param
                + " different from expected\n\tExpected: "
                + str(fields_expected_incoming)
                + "\n\tFound: "
                + str(len(returned_data))
            )

        if (
            returned_data[0] == self.evolver_conf["echo_response_char"]
            and output[1:] != returned_data[1:]
        ):
            raise EvolverSerialError(
                "Error: Value returned by echo different from values sent.\n\tExpected: {}\n\tFound: {}".format(
                    str(output[1:]), str(value)
                )
            )
        elif (
            returned_data[0] != self.evolver_conf["data_response_char"]
            and returned_data[0] != self.evolver_conf["echo_response_char"]
        ):
            raise EvolverSerialError(
                "Error: Incorect response character.\n\tExpected: {}\n\tFound: {}".format(
                    self.evolver_conf["data_response_char"], returned_data[0]
                )
            )

        # ACKNOWLEDGE - lets arduino know it's ok to run any commands (super important!)
        serial_output = [""] * fields_expected_outgoing
        serial_output[0] = self.evolver_conf["acknowledge_char"]
        serial_output = (
            param
            + ",".join(serial_output)
            + ","
            + self.evolver_conf["serial_end_outgoing"]
        )

        serialEvent = Event()
        serialQueue.put(
            {
                "event": serialEvent,
                "payload": bytes(serial_output, "UTF-8"),
                "reply": False,
            }
        )
        serialEvent.wait()

        # This is necessary to allow the ack to be fully written out to samd21 and for them to fully read
        time.sleep(self.evolver_conf["serial_delay"])

        if returned_data[0] == self.evolver_conf["data_response_char"]:
            returned_data = returned_data[1:]
        else:
            returned_data = None

        return returned_data

    def get_num_commands(self) -> int:
        """ """
        return self.command_queue.qsize()

    def process_commands(self, parameters: dict):
        """
        Add all recurring commands and pre/post commands to the command queue
        Immediate commands will have already been added to queue, so are ignored.
        """
        for param, config in parameters.items():
            if config["monitor"]:
                if "pre" in config:  # run this command prior to the main command
                    self.sub_command(config["pre"], parameters)

                # Main command
                self.command_queue.put(
                    {"param": param, "value": config["value"], "type": READ_ONLY}
                )

                if "post" in config:  # run this command after the main command
                    self.sub_command(config["post"], parameters)

    def sub_command(self, command_list: list, parameters: dict):
        """
        Append a list of commands to the command queue.
        """
        for command in command_list:
            parameter = command["param"]
            value = command["value"]
            type = command["type"]
            if value == "values":
                value = parameters[parameter]["value"]
            self.command_queue.put({"param": parameter, "value": value, "type": type})

    def broadcast(self, commands_in_queue: bool) -> dict:
        broadcast_data = {}
        self.clear_broadcast()

        if not commands_in_queue:
            time.sleep(2)
            self.process_commands(self.evolver_conf["experimental_params"])

        # Always run commands so that IMMEDIATE requests occur. RECURRING requests only happen if no commands in queue
        broadcast_data["data"] = self.run_commands()
        broadcast_data["config"] = self.evolver_conf["experimental_params"]

        if 1:  # commands_in_queue:
            broadcast_data["ip"] = self.evolver_conf["evolver_ip"]
            broadcast_data["timestamp"] = time.time()
            broadcastQueue.put("B," + json.dumps(broadcast_data))

        return broadcast_data


class serialPort:
    """
    This class deals with serial port and hardware communication.
    """

    def __init__(self, config: dict):
        self.serial_connection = serial.Serial(
            port=config["serial_port"],
            baudrate=config["serial_baudrate"],
            timeout=config["serial_timeout"],
        )
        self.sleepTime = config["serial_delay"]

    def write(self, payload: bytes):
        """
        Write to serial tty buffer
        """
        self.serial_connection.write(payload)

    def read(self) -> bytes:
        """
        Get reply from serial tty buffer
        """
        return self.serial_connection.readline()

    def serialThread(self):
        """
        This method should be a thread.
        It will handle communication queues (either via serial/common flow or Redis),
        put data into serial port and redirect replies.
        """
        while True:
            request_source = ""

            if serialQueue.qsize:
                request = serialQueue.get()
                request_source = "serial"

            elif redisQueue.qsize:
                request = redisQueue.get()
                request_source = "redis"

            if request_source:
                self.write(request["payload"])

                if request["reply"]:
                    reply = self.read()
                    if not reply:
                        time.sleep(2)
                        self.write(request["payload"])

                    if reply.count(b"end") > 1:
                        reply = reply.split(b"end")[-2] + b"end"

                    if request_source == "serial":
                        serialResponseQueue.put(reply)
                    elif request_source == "redis":
                        redisQueue.put(reply)

                    # Echo reply for broadcast queue, in order to mirror values to Redis database
                    broadcastQueue.put(reply)
                    # Temperature has no echo when sending a setpoint, so:
                    if request["payload"][:5] == b"tempi":
                        broadcastQueue.put(request["payload"])

                time.sleep(self.sleepTime)
                if "event" in request.keys():
                    request["event"].set()

                time.sleep(0.5)

    def run(self):
        """
        Run thread!
        """
        _t1 = Thread(target=self.serialThread)
        _t1.start()


class redisClient:
    """
    This class deals with Redis database
    - As a command source (not used yet)
    - As a broadcast unit
    """

    def __init__(self, config: dict, od_cal_path: str, temp_cal_path: str):
        self.redis_client = redis.Redis(
            config["redis_server_ip"],
            config["redis_server_port"],
            config["redis_server_passwd"],
        )
        self.redis_sirius = redis.StrictRedis(
            "127.0.0.1"
        )  # "10.0.38.46") # Mapping Redis @Sirius for RedisIOC/Archiving WHEN AUTHORIZED! (evolver is not a Sirius application)
        self.od_cal_path = od_cal_path
        self.temp_cal_path = temp_cal_path

    def queueRedisThread(self):
        """
        This thread checks if a command is available at a Redis incoming_queue.
        If so, send it serial queue (redisQueue) and push reply to Redis outcoming_queue.
        """
        while True:
            try:
                while True:
                    # wait until there is a command in the list
                    # command = {"payload": bytes, "reply": boolean}
                    command = json.loads(
                        self.redis_client.brpop(REDIS_INCOMING_QUEUE)[1]
                    )
                    redisQueue.put(command)
                    time.sleep(5)

                    ans = redisQueue.get(block=True)
                    self.redis_client.lpush(REDIS_OUTCOMING_QUEUE, ans)
            except:
                logger.exception("Error in redis queue thread !")

    def broadcastRedisThread(self):
        """
        This thread monitors broadcastQueue (which stores a copy of messages/replies) and save variables into redis keys.
        It stores raw values and also real values after calibration applied.
        """
        while True:
            try:
                while True:
                    # wait until there is a command in the list
                    _info = broadcastQueue.get(
                        block=True
                    )  # .decode('UTF-8', errors='ignore')

                    if type(_info) == bytes:
                        _info = _info.decode("UTF-8", errors="ignore")
                    _param = _info.split(",")[0]

                    if _param == "B":
                        _data = _info[2:]
                        self.redis_client.set("broadcast_data", _data)

                    else:
                        # Get calibrations !
                        with open(CHANNEL_INDEX_PATH) as f:
                            channelIdx = json.load(f)
                        with open(self.temp_cal_path) as f:
                            temp_cal = json.load(f)
                        with open(self.od_cal_path) as f:
                            od_cal = json.load(f)
                            if od_cal["type"] == "3d":
                                od_data_2 = data["data"].get(od_cal["params"][1], None)

                        _data = _info.split(",")[1:17]

                        # If echoing or immediate commands:
                        if ("e" in _param[-1]) or ("i" in _param[-1]):
                            for _ss in range(16):
                                if "stir" in _param[:-1]:
                                    stir_percent = 100 * (float(_data[_ss]) / 4095.0)
                                    self.redis_client.set(
                                        "{}_set_ss_{}".format(_param[:-1], _ss),
                                        stir_percent,
                                    )
                                    self.redis_sirius.set(
                                        "{}_set_ss_{}".format(_param[:-1], _ss),
                                        stir_percent,
                                    )

                                elif "pump" in _param[:-1]:
                                    self.redis_client.set(
                                        "{}_set_ss_{}".format(_param[:-1], _ss),
                                        _data[_ss],
                                    )
                                    self.redis_sirius.set(
                                        "{}_set_ss_{}".format(_param[:-1], _ss),
                                        _data[_ss],
                                    )

                                elif "od_led" in _param[:-1]:
                                    led_percent = 100 * (float(_data[_ss]) / 4095.0)
                                    self.redis_client.set(
                                        "{}_set_ss_{}".format(_param[:-1], _ss),
                                        led_percent,
                                    )
                                    self.redis_sirius.set(
                                        "{}_set_ss_{}".format(_param[:-1], _ss),
                                        led_percent,
                                    )

                                elif "temp" in _param[:-1]:
                                    temp_coefficients = temp_cal["coefficients"][_ss]
                                    try:
                                        temp_value = (
                                            float(_data[_ss]) * temp_coefficients[0]
                                        ) + temp_coefficients[1]
                                        self.redis_client.set(
                                            "{}_set_ss_{}".format(_param[:-1], _ss),
                                            temp_value,
                                        )
                                        self.redis_sirius.set(
                                            "{}_set_ss_{}".format(_param[:-1], _ss),
                                            temp_value,
                                        )
                                    except:
                                        continue

                            self.redis_client.set(
                                "{}_set_timestamp".format(_param[:-1]), time.time()
                            )
                            self.redis_sirius.set(
                                "{}_set_timestamp".format(_param[:-1]), time.time()
                            )

                        # If broadcasting data:
                        elif "b" in _param[-1]:
                            for _ss in range(16):
                                # Here, index maps smart sleeves vs vector channel index
                                index = channelIdx[str(_ss)]["channel"]

                                if "stir" in _param[:-1]:
                                    stir_percent = float(_data[index]) / 4095.0
                                    self.redis_client.set(
                                        "{}_ss_{}".format(_param[:-1], _ss),
                                        stir_percent,
                                    )
                                    self.redis_sirius.set(
                                        "{}_ss_{}".format(_param[:-1], _ss),
                                        stir_percent,
                                    )

                                elif "temp" in _param[:-1]:
                                    temp_coefficients = temp_cal["coefficients"][_ss]
                                    temp_value = (
                                        float(_data[index]) * temp_coefficients[0]
                                    ) + temp_coefficients[1]
                                    self.redis_client.set(
                                        "{}_ss_{}".format(_param[:-1], _ss), temp_value
                                    )
                                    self.redis_sirius.set(
                                        "{}_ss_{}".format(_param[:-1], _ss), temp_value
                                    )

                                elif "od_135" in _param[:-1]:
                                    od_coefficients = od_cal["coefficients"][_ss]
                                    try:
                                        if od_cal["type"] == "sigmoid":
                                            # convert raw photodiode data into ODdata using calibration curve
                                            od_data = np.real(
                                                od_coefficients[2]
                                                - (
                                                    (
                                                        np.log10(
                                                            (
                                                                od_coefficients[1]
                                                                - od_coefficients[0]
                                                            )
                                                            / (
                                                                float(_data[index])
                                                                - od_coefficients[0]
                                                            )
                                                            - 1
                                                        )
                                                    )
                                                    / od_coefficients[3]
                                                )
                                            )
                                            if not np.isfinite(od_data):
                                                od_data = "NaN"
                                        elif od_cal["type"] == "3d":
                                            od_data = np.real(
                                                od_coefficients[0]
                                                + (
                                                    od_coefficients[1]
                                                    * float(_data[_ss])
                                                )
                                                + (
                                                    od_coefficients[2]
                                                    * float(od_data_2[_ss])
                                                )
                                                + (
                                                    od_coefficients[3]
                                                    * (float(_data[_ss]) ** 2)
                                                )
                                                + (
                                                    od_coefficients[4]
                                                    * float(_data[_ss])
                                                    * float(od_data_2[_ss])
                                                )
                                                + (
                                                    od_coefficients[5]
                                                    * (float(od_data_2[_ss]) ** 2)
                                                )
                                            )
                                        else:
                                            logger.error(
                                                "OD calibration not of supported type!"
                                            )
                                            od_data = "NaN"
                                    except ValueError:
                                        print("OD Read Error")
                                        logger.error(
                                            "OD read error for vial %d, setting to NaN"
                                            % x
                                        )
                                        od_data = "NaN"

                                    self.redis_client.set(
                                        "{}_ss_{}".format(_param[:-1], _ss), od_data
                                    )
                                    self.redis_sirius.set(
                                        "{}_ss_{}".format(_param[:-1], _ss), od_data
                                    )

                            self.redis_client.set(
                                "{}_timestamp".format(_param[:-1]), time.time()
                            )
                            self.redis_sirius.set(
                                "{}_timestamp".format(_param[:-1]), time.time()
                            )

            except:
                logger.exception("Error in redis broadcast thread !")

    def run(self):
        """
        Run thread!
        """
        _t1 = Thread(target=self.queueRedisThread)
        _t2 = Thread(target=self.broadcastRedisThread)
        _t1.start()
        _t2.start()
