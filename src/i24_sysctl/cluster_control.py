import sys
import time
import configparser
import socket
import _pickle as pickle
import os
import sys
import json
import multiprocessing as mp
import uuid
import random

from i24_logger.log_writer import logger, catch_critical

# manages connections.. yes, it has a stupid name...
class ServerClient:

    def __init__(self, host, port):
        
        self.host = host
        self.port = port
        
        self.connect_max = 5
        self.connect_cnt = 0
        
        self.sock = None
        
    def connect(self):
    
        # reconnection?
        if self.sock is not None:
            self.sock.close()
            
        try:    
            # create new socket object and connect
            #self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #self.sock.connect((self.host, self.port))
            self.sock = socket.create_connection((self.host, self.port))            
            
            # indicate success
            return True
        
        except ConnectionRefusedError as e:
            print('ConnectionRefused', e)                
        
        # something went wrong, so get rid of the socket
        self.sock = None
        
        #indicate error 
        return False
        
        
    # command-reply transaction (without structure)    
    def transaction(self, payload):    
            
        retry_cnt = 0
        retry_max = 5
        
        while self.sock is not None:
        
            try:
            
                # TODO: check socket status
                # TODO: set timeout
            
                # encode and send
                msg = pickle.dumps(payload)
                self.sock.sendall(msg)
                       
                # receive and decode       
                ret = self.sock.recv(4096)            
                reply = pickle.loads(ret)            
                
                # return with the response
                return reply
                
            except BrokenPipeError as e:
                print('BokenPipe', e)                
                
                # try to reconnect
                retry_cnt += 1
                if retry_cnt <= retry_max:
                    print('Reconect {}/{}'.format(retry_cnt, retry_max))
                    self.connect()
                else:
                    print('Connection lost...')
                    break
                
            except EOFError:
                print('Socket unexpecedly closed')
                
                # try to reconnect
                retry_cnt += 1
                if retry_cnt <= retry_max:
                    print('Reconect {}/{}'.format(retry_cnt, retry_max))
                    self.connect()
                else:
                    print('Connection lost...')
                    break
            
        # indicate the error    
        return None  

    def disconnect(self):
    
        if self.sock:
            self.sock.close()
            self.sock = None
            
            
    def get_peer(self):
        
        if self.sock:
            return self.sock.getpeername()
        else:
            return None
            
    def __repr__(self):
    
        return "ServerClient('{}', {})".format(self.host, self.port)
            
    def __str__(self):
    
        return 'ServerClient ({}:{})'.format(self.host, self.port)
            
            
class ClusterController:


    def __init__(self, config_file):
    
        logger.set_name("ClusterControl")
    
        # parse config to get run settings
        cp = configparser.ConfigParser()
        cp.read(config_file)                                

        # load servers
        self.servers = {}        
        for name, address in dict(cp["SERVERS"]).items():
        
            # extract host, port
            sp = address.split(':')            
            host = sp[0].strip()
            port = int(sp[1])
        
            print(f"'{name}', '{host}', '{port}'")
            
            # create a client for the server
            server = ServerClient(host, port)
            self.servers[name] = server
            
        print(self.servers)
        
    # get server names    
    def server_list(self):
        return self.servers.keys()
        
    # make connection to all servers
    # return: (<bool: all connected>, <dict: name->connected>)
    def connect(self):
        
        ret = {}
        success = True
        
        for name, srv in self.servers.items():
            s = srv.connect()
            success = success and s
            ret[name] = s
            
        return (success, ret)
        
    # disconnect all server    
    def disconnect(self):
        
        for name, srv in self.servers.items():
            srv.disconnect()
                
    
    # send command to all/selected server
    # prefer to use command() if it is possible
    def send_command(self, command, param, server=None):
        
        ret = {}        
               
        if server:
            # send to only one server
            srv = self.servers.get(server)
            
            if srv:
                ret[server] = srv.transaction((command, param))
            
        else:
            # send to all server
            for name, srv in self.servers.items():
                ret[name] = srv.transaction((command, param))
                
        return ret
        
    # convenience function for parsing commands/parameters
    # preferred method for interpreting/sending commands
    def command(self, command, target):
    
        server = None
        param = target # just to be sure..
        
        # target is a string?
        if type(target) is str:        
        
            # try to separate it to server:param
            sp = param.strip().split(':')
            
            
            if len(sp) == 1:
                # single part: some parameter or a server target
                
                if sp[0] in self.servers.keys():
                    # string is a server
                    server = sp[0]
                    param = None
                else:            
                    # string is some parameter
                    server = None
                    param = sp[0]
                    
            elif len(sp) == 2:
                # tuple: target server, parameter pair
                server = sp[0]
                param = sp[1]
        
        # try and convert param to int for PID target
        # PID should only work with a server target to make it unambiguous
        if server:
            try:
                param = int(param)
            except:
                pass
                
        #print('Command:', (command, server, param))
    
        # send the actual command
        return self.send_command(command, param, server)
        
    def get_servers(self):    
        
        ret = {}
        
        # all server
        for name, srv in self.servers.items():
            ret[name] = srv.get_peer()
       
        return ret
        
    # send process list to the designated servers    
    def configure(self, process_list):
    
        ret = []
    
        # do it one-by-one
        for proc in process_list:
        
            srv = self.servers.get(proc['host'])
            
            if srv is not None:
            
                reply = srv.transaction(('CONFIG', [proc]))
                ret.append(reply)
            
            else:
            
                print("No/invalid host '{}' for process {} ({})".format(proc['host'], proc['command'], proc['description']))
                
                ret.append(None)
                
        return ret
            
    
    # load process list from jpl (JSON Process List) file
    # filename: filepath
    # params: dictionary of replaceble parameters ( indicated by $ sign)
    # strict: ignore missing replacement values
    def process_list_from_file(self, filename, params, strict=True):

        try:
    
            # open and decode json
            with open(filename,"rb") as f:
                    processes = json.load(f)
                
            for process in processes:
                
                # replace any $variables with variable values
                if "args" in process.keys():
                    for idx, item in enumerate(process["args"]):
                        if (type(item)==str) and (item[0] == "$"):
                            val = params.get(item[1:])
                            if val is not None:
                                process["args"][idx] = val
                            else:
                                if strict:
                                    raise ValueError('No replacement for "{}"'.format(item[1:]))
                            
                else:                
                    raise KeyError('Malformed JPL file: no "args" list')
                
                # replace any $variables with variable values
                if "kwargs" in process.keys():
                    for key, item in process["kwargs"].items():
                        if (type(item)==str) and (item != "") and (item[0] == "$"):
                            val = params.get(item[1:])
                            if val is not None:
                                process["kwargs"][key] = val
                            else:
                                if strict:
                                    raise ValueError('No replacement for "{}"'.format(item[1:]))
                else:
                    raise KeyError('Malformed JPL file: no "kwargs" dictionary')
                            
                            
            return processes
                        
        except Exception as e:
            
            print('Error during "process_list_from_file"', e)
            
            # indicate error
            return None
        
        
    

class ClusterControl:
    """
    The ClusterControl class controls the entire I24 system. It is itself controlled 
    by two means: 
        1. Static (per run) control configs which are modified once before running
        2. User-keystroke input commands
        
    Set in config:
        - which cameras are to be used
        - which cameras are managed by each state-level node
        - video time at which to start processing 
        - video time at which to end processing
    
    From this set of configs, the FederalControl module generates the necessary config
    files for each state-level system and sends these via tcp to the state-level manager.
    It is assumed that the state level management process is running on each machine
    
    Then, the system control generates the set of processes / inputs that should be run
    on each state-level system and sends these via an additional config file.
    
    Then, the start signal is sent to each state-level manager
    
    Key Commands:
        1.) soft shutdown
        2.) hard shutdown
        
    """
    # generate a MongoDB ObjectID like string
    # uniques is not garanteed!
    # https://www.mongodb.com/docs/v5.2/reference/method/ObjectId/
    def genObjectID(self):
        mac = uuid.getnode()
        t = int(time.time())
        rnd = random.randrange(65535)
        
        return '%08X%06X%02X' % (t, mac, rnd)
    
    # generate some helpful parameters, which can be used with the jobs
    # e.g. new batch ID
    def genHelperParams(self):
    
        pars = {}
        
        pars['OID'] = self.genObjectID()
        
        return pars
        
    
    @catch_critical()
    def __init__(self):
    
        logger.set_name("ClusterControl")

        # check for environment variable
        if "USER_CONFIG_DIRECTORY" not in os.environ:
            raise Exception('Environment variable "USER_CONFIG_DIRECTORY" is not set!')
            
        # make absolute file path    
        filename = "ClusterControl.config"    
        conf_file = os.path.join(os.environ["USER_CONFIG_DIRECTORY"], filename)
        
        # check if config file exists
        if not os.path.exists(conf_file):
            raise FileNotFoundError(conf_file)
        
        # create the controller class
        self.cc = ClusterController(conf_file)
        
        # flag for main loop
        self.run = True
        
        # define command dictionary with command handle,help description
        self.cmd_list = {
            "START"              :"start/restart all processes as needed based on Federal-level run config",
            "FINISH"             : "less urgent graceful shutdown",
            "STOP"               : "graceful shutdown",
            "KILL"               : "immediate shutdown",
            "SOFT_STOP"          : "same as STOP",
            "HARD_STOP"          : "same as KILL",
            "CONFIG"             : "sends new run config",
            "REMOVE"             : "removes processes",
            "LIST_FUNCTIONS"     : "List registered functions",
            "LIST_GROUPS"        : "List process groups",
            "LIST_STATUS"        : "List process status",
            "SHUTDOWN"           : "Remote shutdown of server control",
            "EXIT"               : "Exit control app"                                    
            }              

    # helper function for more (?) user friendy responses
    def pretty_print(self, data):
        print(json.dumps(data, sort_keys=True, indent=2))
        
    # process input string list coming from shell/main loop
    def process_input(self, inp):
    
        # clean input (strip whitespaces), possibly none
        for i in range(0, len(inp)):
            inp[i] = inp[i].strip()
        
        cmd = inp[0]
        target = None
        if len(inp) > 1:
            target = inp[1]
        
        #print('Input:', (cmd, target))
        
        if cmd in ['-h', '--help', "h","H","help","HELP"]:
            print("Valid commands:")
            for key in self.cmd_list:
                print("{} : {}".format(key, self.cmd_list[key]))

        elif cmd == "CONFIG":
        
            if type(target) != str:
                print('Please specify a JPL file!')
                return
               
            # create absolute file path
            # env variable should exists at this point
            jpl_file = os.path.join(os.environ["USER_CONFIG_DIRECTORY"], target + ".jpl")
            
            if not os.path.exists(jpl_file):
                print('JPL file does not exists: ' + jpl_file)
                return
        
            hpars = self.genHelperParams()
        
            # load JPL file
            # TODO: strict check?
            pl = self.cc.process_list_from_file(jpl_file, hpars, False)            
            
            # configure servers
            self.pretty_print(self.cc.configure(pl))
            
        elif cmd == "SERVERS":
            self.pretty_print(self.cc.get_servers())
            
        elif cmd == "EXIT":
            # gracefully exit control app
            self.run = False
        
        elif cmd in self.cmd_list.keys():
            #message = (cmd,target)            
            self.pretty_print(self.cc.command(cmd, target))            
            
        else:
            print("Invalid command.")    

        
    # main function; if no parameters specified it goes to a loop    
    @catch_critical()
    def main(self):
    
        self.cc.connect()
    
        # check mode
        if len(sys.argv) <= 1:
            # no parameters -> interactive mode
    
            self.run = True
    
            while self.run:
                inp = input("Enter command or type 'HELP': ")
                
                # get input and split on whitespaces
                self.process_input(inp.split())
                
        else:
            # parameters present -> shell command mode
            self.process_input(sys.argv[1:])
                    
        self.cc.disconnect()
        

if __name__ == "__main__":    
    
    cc = ClusterControl()
    cc.main()
    
    