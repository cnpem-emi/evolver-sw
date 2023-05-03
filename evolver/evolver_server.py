import serial
import time
import json
import logging
import sys
import os
import yaml
import redis
from threading import Event, Thread
from traceback import print_exc
from queue import Queue

LOCATION = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
IMMEDIATE = 'immediate_command_char'
RECURRING = 'recurring_command_char'
READING = 'reading_command_char'
CALIBRATIONS_FILENAME = "calibrations.json"


# ---- QUEUES
serialQueue = Queue()  # Items in queue: {"payload": bytes, "reply": boolean}
redisQueue = Queue()   #                  payload: bytes to be sent // reply: wait for a reply?
broadcastQueue = Queue()

# ---- REDIS CLIENT
REDIS_INCOMING_QUEUE = "incoming_queue"
REDIS_OUTCOMING_QUEUE = "incoming_queue"


# ---- LOGGING
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s [%(levelname)s] %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S')
logger = logging.getLogger()





class EvolverSerialError(Exception):
    pass



class evolverServer:
    
    def __init__(self, config: dict):
        self.command_queue = Queue()
        self.evolver_conf = config
        self.serial_connection = serial.Serial(port=self.evolver_conf['serial_port'], baudrate = self.evolver_conf['serial_baudrate'], timeout = self.evolver_conf['serial_timeout'])


    def command(self, data: dict) -> dict:
        print('Received COMMAND', flush = True)
        param = data.get('param', None)
        value = data.get('value', None)
        immediate = data.get('immediate', None)
        recurring = data.get('recurring', None)
        fields_expected_outgoing = data.get('fields_expected_outgoing', None)
        fields_expected_incoming = data.get('fields_expected_incoming', None)

        # Update the configuration for the param
        if value is not None:
            if type(value) is list and self.evolver_conf['experimental_params'][param]['value'] is not None:
                for i, v in enumerate(value):
                    if v != 'NaN':
                        self.evolver_conf['experimental_params'][param]['value'][i] = value[i]
            else:
                self.evolver_conf['experimental_params'][param]['value'] = value
        if recurring is not None:
            self.evolver_conf['experimental_params'][param]['recurring'] = recurring
        if fields_expected_outgoing is not None:
            self.evolver_conf['experimental_params'][param]['fields_expected_outgoing'] = fields_expected_outgoing
        if fields_expected_incoming is not None:
            self.evolver_conf['experimental_params'][param]['fields_expected_incoming'] = fields_expected_incoming


        # Save to config the values sent in for the parameter
        with open(os.path.realpath(os.path.join(os.getcwd(),os.path.dirname(__file__), evolver.CONF_FILENAME)), 'w') as ymlfile:
            yaml.dump(self.evolver_conf, ymlfile)

        if immediate:
            self.command_queue.put({'param': param, 'value': value, 'type': IMMEDIATE})
        return(data)


    def getlastcommands(self) -> dict:
        return(self.evolver_conf)


    def getcalibrationnames(self) -> list:
        calibration_names = []
        print("Retrieving cal names...", flush = True)
        try:
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    calibration_names.append({'name': calibration['name'], 'calibrationType': calibration['calibrationType']})
        except FileNotFoundError:
            self.print_calibration_file_error()

        return(calibration_names)


    def getfitnames(self) -> list:
        fit_names = []
        print("Retrieving fit names...", flush = True)
        try:
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    for fit in calibration['fits']:
                        fit_names.append({'name': fit['name'], 'calibrationType': calibration['calibrationType']})
            return(fit_names)
        
        except FileNotFoundError:
            self.print_calibration_file_error()
        

    def getcalibration(self, data: dict) -> dict:
        try:
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    if calibration["name"] == data["name"]:
                        return(calibration)
                        
        except FileNotFoundError:
            self.print_calibration_file_error()


    def setrawcalibration(self, data: dict) -> str:
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
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), 'w') as f:
                json.dump(calibrations, f)
            
            return('success')
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
                            for i, fit in enumerate(calibration['fits']):
                                if fit["name"] == data["fit"]["name"]:
                                    index_to_delete = i
                            if index_to_delete >= 0:
                                del calibrations["fits"][index_to_delete]
                            calibration["fits"].append(data["fit"])
                        else:
                            calibration["fits"] = [].append(data["fit"])
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), 'w') as f:
                json.dump(calibrations, f)
        except FileNotFoundError:
            self.print_calibration_file_error()


    def setactiveodcal(self, data: dict) -> list:
        try:
            active_calibrations = []
            print("Time to set active cals. Data received: ")
            print(data, flush = True)
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    active = False
                    for fit in calibration['fits']:
                        if fit["name"] in data["calibration_names"]:
                            fit["active"] = True
                            active = True
                        else:
                            fit["active"] = False
                    if active:
                        active_calibrations.append(calibration)
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME), 'w') as f:
                json.dump(calibrations, f)

            return(active_calibrations)
        except FileNotFoundError:
            self.print_calibration_file_error()


    def getactivecal(self) -> list:
        try:
            active_calibrations = []
            with open(os.path.join(LOCATION, CALIBRATIONS_FILENAME)) as f:
                calibrations = json.load(f)
                for calibration in calibrations:
                    for fit in calibration['fits']:
                        if fit['active']:
                            active_calibrations.append(calibration)
                            break;
            return(active_calibrations)
        except FileNotFoundError:
            self.print_calibration_file_error()


    def getdevicename(self) -> dict:
        self.config_path = os.path.join(LOCATION)
        with open(os.path.join(LOCATION, self.evolver_conf['device'])) as f:
            configJSON = json.load(f)
        return(configJSON)


    def setdevicename(self, data: dict) -> dict:
        self.config_path = os.path.join(LOCATION)
        print('saving device name', flush = True)
        if not os.path.isdir(self.config_path):
            os.mkdir(self.config_path)
        with open(os.path.join(self.config_path, self.evolver_conf['device']), 'w') as f:
            f.write(json.dumps(data))
        return(data)


    def print_calibration_file_error(self):
        print("Error reading calibrations file.", flush = True)


    def run_commands(self) -> dict:
        data = {}
        while self.command_queue.qsize() > 0:
            command = self.command_queue.get()
            try:
                if command['param'] == 'wait':
                    time.sleep(command['value'])
                    continue
                returned_data = self.serial_communication(command['param'], command['value'], command['type'])
                if returned_data is not None:
                    data[command['param']] = returned_data
            except (TypeError, ValueError, serial.serialutil.SerialException, EvolverSerialError) as e:
                print_exc(file = sys.stdout)
        return data


    def serial_communication(self, param: str, value: list, comm_type: str) -> list:

        output = []

        # Check that parameters being sent to arduino match expected values
        if comm_type == RECURRING:
            output.append(self.evolver_conf[RECURRING])
        elif comm_type == IMMEDIATE:
            output.append(self.evolver_conf[IMMEDIATE])
        elif comm_type == READING:
            output.append(self.evolver_conf[READING])

        if type(value) is list:
            output = output + list(map(str,value))
            for i,command_value in enumerate(output):
                    if command_value == 'NaN':
                        output[i] = self.evolver_conf['experimental_params'][param]['value'][i-1]
        else:
            output.append(value)

        fields_expected_outgoing = self.evolver_conf['experimental_params'][param]['fields_expected_outgoing']
        fields_expected_incoming = self.evolver_conf['experimental_params'][param]['fields_expected_incoming']

        if len(output) is not fields_expected_outgoing:
            raise EvolverSerialError('Error: Number of fields outgoing for ' + param + ' different from expected\n\tExpected: ' + str(fields_expected_outgoing) + '\n\tFound: ' + str(len(output)))

        # Construct the actual string and write out on the serial buffer
        serial_output = param + ','.join(output) + ',' + self.evolver_conf['serial_end_outgoing']
        serialEvent = Event()
        print(serial_output, serialEvent)

        serialQueue.put({"event": serialEvent, "payload": bytes(serial_output, 'UTF-8'), "reply": True})
        serialEvent.wait()

        # Read and process the response
        response = serialQueue.get(block=True).decode('UTF-8', errors='ignore') #.replace(",_!", "end").replace("stiri", "stirb")
        #response = self.serial_connection.readline().decode('UTF-8', errors='ignore')
        print(response, flush = True)
        address = response[0:len(param)]
        if address != param:
            raise EvolverSerialError('Error: Response has incorrect address.\n\tExpected: ' + param + '\n\tFound:' + address)
        if response.find(self.evolver_conf['serial_end_incoming']) != len(response) - len(self.evolver_conf['serial_end_incoming']):
            raise EvolverSerialError('Error: Response did not have valid serial communication termination string!\n\tExpected: ' +  self.evolver_conf['serial_end_incoming'] + '\n\tFound: ' + response[len(response) - 3:])

        # Remove the address and ending from the response string and convert to a list
        returned_data = response[len(param):len(response) - len(self.evolver_conf['serial_end_incoming']) - 1].split(',')

        if len(returned_data) != fields_expected_incoming:
            raise EvolverSerialError('Error: Number of fields received for ' + param + ' different from expected\n\tExpected: ' + str(fields_expected_incoming) + '\n\tFound: ' + str(len(returned_data)))

        if returned_data[0] == self.evolver_conf['echo_response_char'] and \
            output[1:] != returned_data[1:]:
                raise EvolverSerialError('Error: Value returned by echo different from values sent.\n\tExpected: {}\n\tFound: {}'.format(str(output[1:]),str(value)))
        elif returned_data[0] != self.evolver_conf['data_response_char'] and \
            returned_data[0] != self.evolver_conf['echo_response_char']:
                raise EvolverSerialError('Error: Incorect response character.\n\tExpected: {}\n\tFound: {}'.format(self.evolver_conf['data_response_char'], returned_data[0]))

        # ACKNOWLEDGE - lets arduino know it's ok to run any commands (super important!)
        serial_output = [''] * fields_expected_outgoing
        serial_output[0] = self.evolver_conf['acknowledge_char']
        serial_output = param + ','.join(serial_output) + ',' + self.evolver_conf['serial_end_outgoing']
        print(serial_output, flush = True)

        serialQueue.put({"payload": bytes(serial_output, 'UTF-8'), "reply": False})

        # This is necessary to allow the ack to be fully written out to samd21 and for them to fully read
        time.sleep(self.evolver_conf['serial_delay'])

        if returned_data[0] == self.evolver_conf['data_response_char']:
            returned_data = returned_data[1:]
        else:
            returned_data = None

        return returned_data


    def get_num_commands(self) -> int:
        return self.command_queue.qsize()


    def process_commands(self, parameters: dict):
        """
            Add all recurring commands and pre/post commands to the command queue
            Immediate commands will have already been added to queue, so are ignored
        """
        for param, config in parameters.items():
            if config['recurring']: 
                if "pre" in config: # run this command prior to the main command
                    self.sub_command(config['pre'], parameters)

                # Main command
                self.command_queue.put({'param': param, 'value': config['value'], 'type': RECURRING})

                if "post" in config: # run this command after the main command
                    self.sub_command(config['post'], parameters)


    def sub_command(self, command_list: list, parameters: dict):
        """
            Append a list of commands to the command queue
        """
        for command in command_list:
            parameter = command['param']
            value = command['value']
            if value == 'values':
                value = parameters[parameter]['value']
            self.command_queue.put({'param': parameter, 'value': value, 'type': IMMEDIATE})


    

class serialPort:
    def __init__(self, config: dict):
        self.serial_connection = serial.Serial(port=config['serial_port'], baudrate = config['serial_baudrate'], timeout = config['serial_timeout'])
        self.sleepTime = config['serial_delay']

    def write(self, payload: bytes):
        self.serial_connection.write(payload)

    def read(self) -> bytes:
        return(self.serial_connection.readline())
    
    def serialThread(self):
        while True:
            request_source = ""

            if serialQueue.qsize:
                request = serialQueue.get()
                request_source = 'serial'
                
            elif redisQueue.qsize:
                request = redisQueue.get()
                request_source = 'redis'

            if request_source:
                self.write(request["payload"])
                if(request["reply"]):
                    reply = self.read()
                    if reply.count(b'end') > 1:
                        reply = reply.split(b'end')[-2]+b'end'

                    if request_source == 'serial':
                        serialQueue.put(reply)
                    elif request_source == 'redis':
                        redisQueue.put(reply)
                    broadcastQueue.put(reply)
                    if "event" in request.keys():
                        request["event"].set()

            time.sleep(self.sleepTime)

    def run(self):
        _t1 = Thread(target=self.serialThread)
        _t1.start()



class redisClient:
    def __init__(self, config: dict):
        self.redis_client = redis.Redis(config["redis_server_ip"], config["redis_server_port"], config["redis_server_passwd"]) 

    def queueRedisThread(self):
        while(True):
            try:
                while(True):
                    # wait until there is a command in the list
                    # command = {"payload": bytes, "reply": boolean}
                    command = json.loads(self.redis_client.brpop(REDIS_INCOMING_QUEUE)[1])
                    redisQueue.put(command)
                    self.redis_client.lpush(redisQueue.get(block=True))
            except:
                logger.exception('Error in redis queue thread !')


    def broadcastRedisThread(self):
        while(True):
            try:
                while(True):
                    # wait until there is a command in the list
                    _info = broadcastQueue.get(block=True).decode('UTF-8', errors='ignore')
                    print(_info)
                    _param = _info.split(",")[0]
                    _data = _info.split(",")[1:17]
                  
                    print(_param[-1])
                    if 'i' in _param[-1]:
                        for _ss in range(16):
                            self.redis_client.set("{}_ss_{}".format(_param[:-1], _ss), _data[_ss])

            except:
                logger.exception('Error in redis broadcast thread !')


    def run(self):
        _t1 = Thread(target=self.queueRedisThread)
        _t2 = Thread(target=self.broadcastRedisThread)
        _t1.start()
        _t2.start()