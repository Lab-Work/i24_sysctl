import sys
import time
import configparser
import socket
import _pickle as pickle
import os
import sys
import json
import subprocess
import multiprocessing as mp

from i24_logger.log_writer import logger, catch_critical, log_errors
logger.set_name("ServerControl")




class ServerControl:
    """
    ServerControl has a few main functions. 
    1. Continually open socket (server side) that listens for commands from ClusterControl
    2. Start and maintain a list of subprocesses and processes
    3. Monitor these processes and restart them as necessary
    4. Log any status changes
    """
    
    
    def __init__(self,sock_port = 5999):
        #logger.set_name("ServerControl")

        self.process_lookup = {} # TODO implement this somehoww
        self.TCP_dict = {} # which function to run for each TCP message
        
        self.process_list = []
        
        
        # create socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        server_address = (local_ip,sock_port)
        self.sock.bind(server_address)
        self.sock.listen(1)
                
        # Wait for a connection
        try:
            self.connection, self.client_address = self.sock.accept()
            logger.debug("ServerControl connected to ClusterControl at {}".format(self.client_address))
        except:
            self.cleanup()
            
        self.main()
        
    def init_procs(self,msg):
        # Wait for a connection
        self.process_list = msg
    
        # start processes
        for proc in self.process_list:
            if proc["mode"] == "subprocess":
                self.init_subproc(proc)
            elif proc["mode"] == "process":
                self.init_proc(proc)
                
        logger.debug("Initialized {} processes".format(len(self.process_list)))
            
       
            
    def init_proc(self,proc):
        
        # replace process name with actual python process
        proc["command"] = self.process_lookup(proc["command"])
        
        actual_process = mp.Process(target = proc["command"],args = proc["args"],kwargs = proc["kwargs"])
        proc["process"] = actual_process
    
    def init_subproc(self,proc): 
        # start subprocess
        command = proc["command"].split(" ") #+ proc["args"]
        print("COMMAND: {}".format(command))
        proc["process"] = subprocess.Popen(command,shell = True)
        
    def proc_status_check(self): 
        for proc in self.process_list:
            status = proc["process"].status()
            if status is Dead:
                # log dead process to logger
                logger.warning("Process {} died and is being restarted.".format(proc["command"]))
                
                # terminate and join process
                
                # remove from list
                
                # restart a new Process with the same command
                
                
    
    def cleanup (self): 
        """ Close socket, log shutdown, etc"""
        logger.debug("Cleaning up sockets")
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.connection.close()

        
    def send_signal(self): pass
    
    def recv_msg(self,timeout = 0.01):
        self.connection.settimeout(timeout)

        try:
            payload = self.connection.recv(4096)
            return pickle.loads(payload)
        except socket.timeout:
            return None
        
    @catch_critical()   
    def main(self):
        print("Got to main")

        while True:
            try:
                msg = self.recv_msg()
                
                if msg is None:
                    continue
                
                else:
                    print(msg)
                    
                # parse configs from ClusterControl
                if type(msg) == list:
                    
                    if len(self.process_list) > 0:
                        logger.warning("Received new process config from ClusterControl even though ServerControl already has a set of processes to control. May result in orphaned processes.")  
                    self.init_procs(msg)
        
                # parse commands from ClusterControl
                elif type(msg) == tuple and len(msg) == 2:
                    command = msg[0]
                    group = msg[1]
                    
                    # take action on relevant group
                    
                
                # Keep proceses alive
        
            except Exception as e:
                if type(e).__name__ == "EOFError":
                    continue
                else:
                    self.cleanup()
                    raise e
                    break

if __name__ == "__main__":
    
    s = ServerControl()

