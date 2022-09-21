import sys
import time
import configparser
import socket
import selectors
import _pickle as pickle
import os
import sys
import json
import subprocess
import multiprocessing as mp
import signal
import traceback
import importlib

from i24_logger.log_writer import logger, catch_critical, log_errors        
        
        
# CODEWRITER TODO - import your process targets here such that these functions can be directly passed as the target to mp.Process or mp.Pool

# TODO - add your process targets to register_functions
# register_functions = [dummy_function]
# name_to_process = dict([(fn.__name__, fn) for fn in register_functions])

# all jobs have this format
processContainer_exampele = {
    "host": "laptop",
    "command": "name of target function",
    "args": [],
    "kwargs": {}, 
    "abandon": False,
    "timeout": 10.0,
    "restart_max": 5,
    "group": "group name e.g. TRACKING",
    "description": "Some description to make sense to human eyes..",
    }

# Managager class for 'multiprocess.Process' based processes
class ProcessMP:

    # aargs: additional_args
    # name2proc: name_to_process
    def __init__(self, pC, aargs, name2proc):
    
        self.process = None # mp.Process
        self.pid = -1 # PID, for better access
        self.command = pC['command'] # function name
        self.group = pC['group']
        self.description = pC['description']
        
        # external process parameters
        self.e_args = pC['args']
        self.e_kwargs = pC['kwargs']
        self._daemon = not pC['abandon']
        
        # full process parameters (internal and external)                
        self.p_target = name2proc[self.command]
        if self.command in aargs.keys():
            self.p_args = aargs[self.command][0] + self.e_args
            self.p_kwargs = {**aargs[self.command][1], **self.e_kwargs}
        else:
            self.p_args = self.e_args
            self.p_kwargs = self.e_kwargs 

        # process management policies
        self.timeout = pC['timeout']
        self.restart_max = pC['restart_max']        
        
        # process management variables
        self.keep_alive = False
        self.kill_time = 0
        self.start_count = 0
        self.start_time = 0 # start time from monotonic clock
        self.uptime = 0 # uptime from last start; updated by manage() function (time resolution: timeout) (TODO: update only on query or stop?)
        
    # start process
    def start(self):
    
        # TODO: check process state / hande exception for process.close()
        
        # have previous process?
        if self.process:
            self.process.close()
        
        # create Process
        self.process = mp.Process(target = self.p_target,args = self.p_args,kwargs = self.p_kwargs, daemon = self._daemon)
        
        self.keep_alive = True
        
        # start process
        self.process.start()
        
        # store PID
        self.pid = self.process.pid
        
        # process management
        self.keep_alive = True
        self.start_count += 1
        self.kill_time = 0
        self.start_time = time.monotonic()        
        
    # finish all processing before exit    
    def finish(self):
        
        # process management
        self.keep_alive = False
        
        if self.is_alive():
        
            # send SIGUSR1
            os.kill(self.process.pid, signal.SIGUSR1)
            
            # TODO exception handling
        

    # exit as soon as possible
    def stop(self):
    
        # process management
        self.keep_alive = False
        self.kill_time = time.time() + self.timeout
    
        if self.is_alive():
        
            # send SIGINT
            os.kill(self.process.pid, signal.SIGINT)
            
            # TODO exception handling
        
    # terminate process    
    def kill(self):
    
        # process management
        self.keep_alive = False    
    
        if self.is_alive():
        
            #send SIGKILL
            os.kill(self.process.pid, signal.SIGKILL)
            
            # TODO exception handling
            
    
    # call this method periodically to manage the process (keep alive / terminate / etc.)
    def manage(self):
    
    
        if self.is_alive():
        
            # update uptime
            self.uptime = round(time.monotonic() - self.start_time)
        
            # terminate the process if needed
        
            if (self.kill_time > 0) and (self.kill_time > time.time()):
                self.kill()
        
        else:        
            # resurrect the process if needed
            
            if self.keep_alive and self.start_count <= self.restart_max:
                self.start()
            
        
            
        
    # convenience function to check aliveness    
    def is_alive(self):
        
        if self.process:
            return self.process.is_alive()
            
        return False

    # retrieve status information (dictionary)
    def status(self):
    
        stat = {}
        
        stat['command'] = self.command
        stat['name'] = self.description
        stat['group'] = self.group
        stat['pid'] = self.pid
        stat['start_count'] = self.start_count
        stat['alive'] = self.is_alive()
        stat['uptime'] = self.uptime
        
        return stat
        
    def __str__(self):
        return str(self.status())
    

class ServerControl:
    """
    ServerControl has a few main functions. 
    1. Continually open socket (server side) that listens for commands from ClusterControl
    2. Start and maintain a list of subprocesses and processes
    3. Monitor these processes and restart them as necessary
    4. Log any status changes
    """
    
    def get_additional_args(self):
        # CODEWRITER TODO - Implement any shared variables (queues etc. here)
        # each entry is a tuple (args,kwargs) (list,dict)
        # to be added to process args/kwargs and key is process_name        
        return {}
    
    def __init__(self, filename, name_to_process):
    
        self.name_to_process = name_to_process # which target function to use for string process name
        
        logger.set_name("{} Server Control".format(str(socket.gethostname()).upper()))
    
        # check for environment variable
        if "USER_CONFIG_DIRECTORY" not in os.environ:
            raise Exception('Environment variable "USER_CONFIG_DIRECTORY" is not set!')
            
        # make absolute file path        
        conf_file = os.path.join(os.environ["USER_CONFIG_DIRECTORY"], filename + '.srv')
        
        # check if config file exists
        if not os.path.exists(conf_file):
            raise FileNotFoundError(conf_file)    
    
        # parse config to get run settings
        cp = configparser.ConfigParser()
        cp.read(conf_file)

        settings = dict(cp['SETTINGS'])
        
        self.name = settings['name']
        
        # server access configurations
        self.host = settings['host']
        self.port = int(settings['port'])
    
        self.log_frequency = float(settings['log_frequency']) # every _ seconds
        self.last_log = 0
        self.default_timeout = 5
        
        # get dynamically loaded modules
        mods = dict(cp['MODULES'])
        
        self.modules = {}
        
        # configure / load specified modules
        for key, val in mods.items():
        
            try:
                # get directory/package path and module name
                (mod_dir, mod_name) = val.split('|')
                mod_dir = mod_dir.strip()
                mod_name = mod_name.strip()
                
                mod_file = os.path.join(mod_dir, mod_name + '.py')
                
                if not os.path.exists(mod_file):
                    raise FileNotFoundError('Module not exists: ' + mod_file)
                    
                # load module
                sys.path.append(mod_dir)
                mod = importlib.import_module(mod_name)
                
                if '__process_entry__' not in mod.__dir__():
                    raise NotImplementedError('Module "{}" does not have the required "__process_entry__" function!'.format(mod_name))
                
                # TODO: call __process_setup__
                
                self.name_to_process[key] = mod.__process_entry__
                
                # save module for later use/reload
                self.modules[key] = (mod_dir, mod_name, mod)
                
                print('Module "{}" from "{}" added'.format(mod_name, mod_dir))
                
            except Exception as e:
                print(e)
            
        print(self.modules)
        
        
        # self main loop flag
        self.run = True
        
        # start time for monitoring uptime
        self.start_time = time.monotonic()
        
        # counter for installed jobs; used for creating negative PID values
        self.jobCount = 0
        
        # select for multiconnection
        self.select = selectors.DefaultSelector()
        
        self.msg_to_fn = {
                         "CONFIG":self.cmd_configure,
                         "REMOVE":self.cmd_remove,
                         "START":self.cmd_start,
                         "FINISH": self.cmd_finish,
                         "STOP": self.cmd_stop,
                         "KILL": self.cmd_kill,
                         "SOFT_STOP":self.cmd_stop, # alternate command name
                         "HARD_STOP":self.cmd_kill, # alternate command name
                         "LIST_FUNCTIONS": self.cmd_list_functions,
                         "LIST_GROUPS": self.cmd_list_groups,
                         "LIST_STATUS": self.cmd_list_status,
                         "SHUTDOWN":self.cmd_shutdown, # server shutdown
                         } # which function to run for each TCP message                        
        
        # to store processContainers
        self.process_list = []
        
        # process groups
        self.process_groups = []        
        
        # CODEWRITER TODO - Implement any shared variables (queues etc. here)
        # each entry is a tuple (args,kwargs) (list,dict)
        # to be added to process args/kwargs and key is process_name
        self.additional_args = self.get_additional_args()
                                
    
    #%% SOCKET RELATED FUNCTIONS
    #
    #
    
    # socket event: accept new connections
    def accept_connection(self, sel):
        sock = sel.fileobj
        conn, addr = sock.accept()  # Should be ready to read
        print(f"Accepted connection from {addr}")
        conn.setblocking(False)
        #data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        data = f"Client {addr}"
        print("Data", data)
        self.select.register(conn, selectors.EVENT_READ, data=data)
        
        print('Select:', self.select.get_map())
        for key, val in self.select.get_map().items():
            print('Map:', key, val.fileobj)   
            
            
    # socket event: handle new data
    def service_connection(self, sel, mask):
        sock = sel.fileobj
        data = sel.data
        if mask & selectors.EVENT_READ:
        
            try:
                recv_data = sock.recv(1024)  # Should be ready to read
                if recv_data:
                    #print("Received:", recv_data)
                    
                    # decode message
                    msg = pickle.loads(recv_data)
                    cmd = msg[0]
                    
                    # default error message
                    reply = (False, "Command call error")
                    
                    if cmd in self.msg_to_fn:
                    
                        # execute command
                        retval = self.msg_to_fn[cmd](msg)
                        print('Command response:', retval)
                        
                        # check command response
                        if (type(retval) is tuple) and len(retval) == 2:
                        
                            # time to make the reply message                        
                            reply = retval
                        
                    else:
                        # error message
                        reply = (False, "Unsupported command '%s'" % cmd)
                    
                    # send back response
                    sock.sendall(pickle.dumps(reply))
                    
                    # echo
                    #sock.send(recv_data)
                    
                else:
                    # socket closed by client..
                    # cleanup time
                    print(f"Closing connection to {data}")
                    self.select.unregister(sock)
                    sock.close()  
            
            except Exception as e:
                # unexpected error                
                # recovery strategy: disconnect client
                
                stacktrace = traceback.format_exc()                                
                logger.error("Unhandled exception in 'service_connection'", extra={"stacktrace":stacktrace})
                
                print(f"Force closing connection to {data}")
                self.select.unregister(sock)
                sock.close()  
            
                
    # socket event: handle select timeout
    def handle_timeout(self):
        
        # periodic log
        if (time.time() - self.last_log) > self.log_frequency:
            self.log_status()
    
        # TODO process timeouts
        # ...
        
        # manage processes
        for proc in self.process_list:
            proc.manage()
        
    #%% MESSAGE HANDLER PROCESSES
    # functions should return tuple (success, ret)
    # success - boolean indicating the success
    # ret - custom return value

    def cmd_configure(self,msg):
    
        count = 0
    
        # add each processContainer to self.process_list
        for pC in msg[1]:  
        
            # check if we have the requested process registered
            # other checks are done at object creation
            if pC['command'] not in self.name_to_process:
                return (False, "Unsupported function '{}'".format(pC['command']))

            # TODO handle exception during creation

            # create new process object
            proc = ProcessMP(pC, self.additional_args, self.name_to_process)
            
            # add unique negative PID for selection
            self.jobCount += 1
            proc.pid = -1 * self.jobCount
        
            self.process_list.append(proc)
            print(proc)
            
            # collect process group
            if proc.group not in self.process_groups:
                self.process_groups.append(proc.group)
                
            count += 1
            
        logger.debug("Initialized {} new process(es). Total: {}".format(count, len(self.process_list)))
        
        return (True, None)    
        
    def cmd_remove(self, msg):
    
        count = 0
        
        # we might modify the original process_list -> use a copy for iterration
        processes = self.select_processes(msg[1], False).copy()
    
        for proc in processes:
        
            # TODO check process state before removal
            # active process: stop / kill / leave?
        
            # remove process
            self.process_list.remove(proc)
            
            count += 1
            
        # TODO: recollect group list
            
        logger.debug("Removed {} processes with target: {}".format(count, msg[1]))

        return (True, None)        
        
    def cmd_start(self, msg):
    
        count = 0
    
        for proc in self.select_processes(msg[1], False):
        
            # TODO: check process state before start
        
            # start process
            proc.start()
            count += 1
            
        logger.debug("Started {} processes with target: {}".format(count, msg[1]))

        return (True, None)       
    
    def cmd_finish(self, msg):
    
        count = 0
        
        for proc in self.select_processes(msg[1], False):                    
        
            # finish process
            proc.finish()
            count += 1
            
            # TODO: check process state after kill?
            
        logger.debug("Sent SIGUSR1 signal (FINNISH command) to {} processes with group: {}".format(count, msg[1]))

        return (True, None)    
        
    def cmd_stop(self, msg):
    
        count = 0
        
        for proc in self.select_processes(msg[1], False):                    
        
            # stop process
            proc.stop()
            count += 1
            
            # TODO: check process state after kill?
            
        logger.debug("Sent SIGINT signal (STOP command) to {} processes with group: {}".format(count, msg[1]))

        return (True, None)    

    def cmd_kill(self, msg):
    
        count = 0
        
        for proc in self.select_processes(msg[1], False):                    
        
            # start process
            proc.kill()
            count += 1
            
            # TODO: check process state after kill?
            
        logger.debug("Sent KILL signal (KILL command) to {} processes with group: {}".format(count, msg[1]))

        return (True, None)       
        
    # list registered functions    
    def cmd_list_functions(self, msg):
    
        f_list = [];
    
        for proc in self.name_to_process:
            f_list.append(proc)
            
        return (True, f_list)
    
    # list process groups
    def cmd_list_groups(self, msg):
    
        return (True, self.process_groups)
        
    # show status of the configured processes    
    def cmd_list_status(self, msg):    
        
        ret = {}
        p_list = [];
        
        for proc in self.select_processes(msg[1], True):
            p_list.append(proc.status())
            
        ret['proc'] = p_list
        
        ret['uptime'] = round(time.monotonic() - self.start_time)
            
        return (True, ret)       
                    
        
    # gracefully/remotely shutdown the ServerControl
    def cmd_shutdown(self, msg):
        self.run = False
        
        # TODO: kill processes???
        
        return (True, None)
    
    #%% Assorted OTHER FUNCTIONS

    def log_status(self):
        n_live = 0
        to_log = {}
        for proc in self.process_list:
            if proc.is_alive():
                n_live += 1
                to_log["PID:" + str(proc.pid)] = [proc.command, proc.group, proc.pid]
                
        logger.info("{} live processes".format(n_live),extra = to_log)
        self.last_log = time.time()
    
    # target: PID, group, function, all
    # safe: True: returns all on both target==None or 'all', False: returns all only on target=='all'
    # 'safe' should be False for dangerous operations e.g. START, STOP, KILL
    # 'safe' can be True for harmless operations e.g. LIST_*
    def select_processes(self, target, safe=False):
        
        if (target=='all') or (safe and (target is None)):
            return self.process_list
            
        p_list = []
            
        # select by PID    
        if type(target) == int:
            
            for proc in self.process_list:
                if proc.pid == target:
                    p_list.append(proc)
                    
            return p_list
            
        # selecct by process group
        if (type(target) == str) and (target in self.process_groups):
            
            for proc in self.process_list:
                if proc.group == target:
                    p_list.append(proc)
                    
            return p_list
            
        # select by function name
        if (type(target) == str) and (target in self.name_to_process):
            
            for proc in self.process_list:
                if proc.command == target:
                    p_list.append(proc)
                    
            return p_list
            
        # no match or unknow select criteria
        return []
        
    
    #%% MAIN LOOP
    @catch_critical()   
    def main(self):
    
        # create server socket, bind, and listen
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # ugly fix.. TODO: close connections properly
        sock.bind((self.host, self.port))
        sock.listen()
        sock.setblocking(False)
        self.select.register(sock, selectors.EVENT_READ, data=None)
        
        print(f"Server '{self.name}'")
        print(f"Listening on {(self.host, self.port)}")

        print('Registered processes:')
        for name in self.name_to_process:
            print(name)

        while self.run:
            events = self.select.select(timeout=2)
            if events:          
                for sel, mask in events:
                    if sel.data is None:
                        self.accept_connection(sel)
                    else:
                        self.service_connection(sel, mask)
                        
            else:
                # regular timeout
                self.handle_timeout()

        print('Graceful shutdown?!...')

        # cleanup        
        self.select.close()        
        
        # TODO: closing server/client sockets?
        
        # delay for proper system cleanup (?)
        time.sleep(1)
        
        print('Bye! Bye!')
    
    
if __name__ == "__main__":

    print(sys.argv)

    if len(sys.argv) == 2:
    
        # create server
        s = ServerControl(sys.argv[1], {})
        
        # start server
        s.main()
        
    else:
    
        print('Error: configuration not specified!') 
        



