import sys
import time
import configparser
import socket
import _pickle as pickle
import os
import sys
import json
import multiprocessing as mp

from i24_logger.log_writer import logger, catch_critical
logger.set_name("ClusterControl")


def dummyServer():
    try:
        socks = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ("10.2.219.150",5993)
        socks.bind(server_address)
        
        # Listen for incoming connections
        socks.listen(1)
        
        # Wait for a connection
        connection, client_address = socks.accept()
    
        time.sleep(60)
        connection.close()
        
    except Exception as e:
        if type(e).__name__ == "KeyboardInterrupt":
            pass
        else:
            print(type(e).__name__)
            socks.shutdown(socket.SHUT_RDWR)
            socks.close()
            connection.close()
            raise Exception("Closed dummyServer Socket")
    
#pr = mp.Process(target = dummyServer,args = ())
#pr.start()

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
    
    @catch_critical()
    def __init__(self,run_config_file,process_config_directory):
        """
        :param run_config_file - (str) path to federal-level run config file
        """
        logger.set_name("ClusterControl")

        
        # parse config to get run settings
        cp = configparser.ConfigParser()
        cp.read(run_config_file)
        
        self.params = dict(cp["PARAMETERS"])
        self.params = dict([(key.upper(),self.params[key]) for key in self.params]) # make  parameter names all-uppercase
        
        self.servers = dict(cp["SERVERS"])
        self.servers = dict([(key,(self.servers[key].split(":")[0],int(self.servers[key].split(":")[1]))) for key in self.servers])
        
        # generate config files for each node
        self.configs = self.generate_configs(process_config_directory)
        
        # establish TCP connections with each state-level node (I am the client)
        # self.sockets is keyed by server names as specified in ClusterControl.config
        self.sockets = {}
        for server in self.servers.keys():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Connect the socket to the port where the server is listening
            server_address = (self.servers[server])
            sock.connect(server_address)
            self.sockets[server] = sock
        
        # define command dictionary with command handle,help description
        self.cmd = {
            "START"              :"start/restart all processes as needed based on Federal-level run config",
            "FINISH PROCESSING"  : "less urgent graceful shutdown",
            "SOFT STOP"          : "component-implemented graceful shutdown",
            "HARD STOP"          : "default state-level process termination",
            "STOP"               : "same as soft stop",
            "CONFIG"             : "sends new run config."
            }
      
    @catch_critical()
    def sock_send(self,payload,server_name):
        msg = pickle.dumps(payload)
        self.sockets[server_name].sendall(msg)
        
    @catch_critical()
    def generate_configs(self,process_config_directory):
        """
        The current implementation assumes static camera-to-machine mapping, 
        and assumes that each state-level machine manages its own hardware devices 
        (camera-to-GPU mapping). Thus, this config consists of a list of processes
        and arguements to be run by the state-level manager. Note that it is expected that 
        self.params contains values for all of the $demarcated variable names in each JSON
        
        :param process_config_directory - (str) directory with a json-style set of processContainers for each server
        
        processContainer = {
            "command": "name of target function",
            "timeout": 1,
            "args": [],
            "kwargs": {}, 
            "group": "INGEST" or "TRACKING" or "POSTPROCESSING" or "ARCHIVE",
            "description": "This process specifies 2 arguments, 0 keyword arguments and 0 flags at the Cluster Level. It expects 2 additional arguments and one additional keyword argument to be appended by ServerControl"
            }
        
        :returns None
        """
        
        configs = {}
        
        files = os.listdir(process_config_directory)
        files = [os.path.join(process_config_directory,file) for file in files]
        
        process_list = []
        for file in files:
            with open(file,"rb") as f:
                processes = json.load(f)
            
            for process in processes:
                
                # replace any $variables with variable values
                if "args" in process.keys():
                    for item in process["args"]:
                        if item[0] == "$":
                            item = self.params[item[1:]]
                if "kwargs" in process.keys():
                    for item in process["kwargs"]:
                        if item[0] == "$":
                            item = self.params[item[1:]]

                        
                # append to process_list
                process_list.append(process)
                
            server_name = file.split("/")[-1].split(".")[0]
            configs[server_name] = process_list
            
    
        logger.debug("Generated state-level run configs")
        return configs
    
    
    @catch_critical()
    def send_configs(self):
        for server in self.configs.keys():
            message = ("CONFIG",self.configs[server])
            self.sock_send(message,server)

        logger.debug("Sent run configs to all active ServerControl modules")
        
        

        
    def send_message(self,message):
        for server in self.servers:
                self.sock_send(message, server)
        logger.debug("Sent command {} to all active ServerControl modules".format(message))
    
        
    @catch_critical()
    def main(self):
        while True:
            inp = input("Enter command or press (h) for list of valid commands: ")
            
            inp = inp.split(",")
            group = None
            if len(inp) > 1:
                group = inp[1]
            inp = inp[0]
            print(inp,group)
            
            if inp in ["h","H","help","HELP"]:
                print("Valid commands:")
                for key in self.cmd:
                    print("{} : {}".format(key,self.cmd[key]))

            elif inp == "CONFIG":
                self.send_configs()
            
            elif inp in self.cmd.keys():
                message = (inp,group)
                self.send_message(message)
            
            else:
                print("Invalid command. Re-entering waiting loop...")
                time.sleep(1)
                print("Press ctrl+C to enter a commmand")
                    



    

if __name__ == "__main__":
    
    run_config_file = "/home/derek/Documents/i24/i24_sysctl/config/ClusterControl.config"
    process_config_directory = "/home/derek/Documents/i24/i24_sysctl/config/servers"                  
    c = ClusterControl(run_config_file, process_config_directory)
    c.main()