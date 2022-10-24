from i24_sysctl import ServerControl
#from i24_sys import ServerControlStub
import time
import os
import signal
import sys
from i24_logger.log_writer import logger



# CODEWRITER TODO - import your process targets here such that these functions can be directly passed as the target to mp.Process or mp.Pool


# 'normal'/expected behaviour
# SIGINT: exit imediately
# SIGUSR: potential wait
class NormalProcess:

    def __init__(self, index=0):
        self.index = index
        print('NormalProcess {}: created (PID={})'.format(self.index, os.getpid()))
        self.run = True      
        
    def sigusr_handler(self, sig, frame):
        print('NormalProcess {}: SIGUSR received'.format(self.index))
        
        self.run = False;
        
        
    def sigint_handler(self, sig, frame):
        print('NormalProcess {}: SIGINT received'.format(self.index))
        
        # nothing to cleanup so we can make an immediate exit
        print('NormalProcess {}: exit'.format(self.index))
        sys.exit(0)
        
        
    def main(self):
    
        print('NormalProcess {}: main'.format(self.index))
    
        while self.run:
            print('Normal process {}...'.format(self.index))
            time.sleep(5)  

        print('NormalProcess {}: graceful exit'.format(self.index))        
        
# catch all signals, but do not respond    
class RebelProcess:

    def __init__(self):
        print('RebelProcess: created', os.getpid())
        self.run = True
        
    def sigusr_handler(self, sig, frame):
        print('RebelProcess: SIGUSR received')
        
        # do nothing
        
    def sigint_handler(self, sig, frame):
        print('RebelProcess: SIGINT received')
        
        # do nothing
        
    def main(self):
    
        print('RebelProcess: main')
    
        while self.run:
            print('Rebel process...')
            time.sleep(5)  

        print('Never reach this!')
        
# responsive but too slow..
class SlowProcess:

    def __init__(self):
        print('SlowProcess: created', os.getpid())
        self.run = True
        
    def sigusr_handler(self, sig, frame):
        print('SlowProcess: SIGUSR received')
        
        time.sleep(10)
        
        self.run = False;
        
        
    def sigint_handler(self, sig, frame):
        print('SlowProcess: SIGINT received')
        
        # nothing to cleanup so we can make an immediate exit
        print('SlowProcess: exit')
        sys.exit(0)
        
    def main(self):
    
        print('SlowProcess: main')
    
        while self.run:
            print('Slow process...')
            time.sleep(5)  

        print('SlowProcess: graceful exit')
        
# process silently dying after some time...       
class SickProcess:

    def __init__(self, life=0):
        self.life = life
        print('SickProcess with lifetime {}: created (PID={})'.format(self.life, os.getpid()))
        self.run = True      
        
    def sigusr_handler(self, sig, frame):
        print('SickProcess (PID={}): SIGUSR received'.format(os.getpid()))
        
        self.run = False;
        
        
    def sigint_handler(self, sig, frame):
        print('SickProcess (PID={}): SIGINT received'.format(os.getpid()))
        
        # nothing to cleanup so we can make an immediate exit
        print('SickProcess (PID={}): exit'.format(os.getpid()))
        sys.exit(0)
        
        
    def main(self):
    
        print('SickProcess, with lifetime {}: main'.format(self.life))
        
        timeout = time.time() + self.life
    
        while self.run and timeout > time.time():
            print('Sick process, remaining {}...'.format(timeout- time.time()))
            time.sleep(5)  

        print('SickProcess (PID={}): graceful exit'.format(os.getpid())) 
        
        return 77

# process dying with an exception after some time...       
class ExplodingProcess:

    def __init__(self, life=0):
        self.life = life
        print('ExplodingProcess with lifetime {}: created (PID={})'.format(self.life, os.getpid()))
        self.run = True      
        
    def sigusr_handler(self, sig, frame):
        print('ExplodingProcess (PID={}): SIGUSR received'.format(os.getpid()))
        
        self.run = False;
        
        
    def sigint_handler(self, sig, frame):
        print('ExplodingProcess (PID={}): SIGINT received'.format(os.getpid()))
        
        # nothing to cleanup so we can make an immediate exit
        print('ExplodingProcess (PID={}): exit'.format(os.getpid()))
        sys.exit(0)
        
        
    def main(self):
    
        print('ExplodingProcess, with lifetime {}: main'.format(self.life))
        
        timeout = time.time() + self.life
    
        while self.run and timeout > time.time():
            print('Exploding process, remaining {}...'.format(round(timeout- time.time(), 2)))
            time.sleep(5)  
            
        print('Explosion (PID={})!'.format(os.getpid())) 

        raise Exception('Violent dying of ExplodingProcess')

        print('ExplodingProcess (PID={}): graceful exit ?!'.format(os.getpid())) 

def normal_proc(arg1, arg2, kwarg1 = None):

    p = NormalProcess(kwarg1)
    
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    p.main()
    
def rebel_proc(arg1, arg2, kwarg1 = None):

    p = RebelProcess()
    
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    p.main()

def slow_proc(arg1, arg2, kwarg1 = None):

    p = SlowProcess()
    
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    p.main() 

def sick_proc(arg1, arg2, lifetime = None):

    p = SickProcess(lifetime)
    
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    p.main() 

def exploding_proc(arg1, arg2, lifetime = None):

    p = ExplodingProcess(lifetime)
    
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    p.main()     
    
    
import sys
import importlib
    

# CODEWRITER TODO - add your process targets to register_functions
register_functions = [normal_proc, rebel_proc, slow_proc, sick_proc, exploding_proc]
name_to_process = dict([(fn.__name__, fn) for fn in register_functions])



class TestServerControl(ServerControl):
    
    def get_additional_args(self):
        # CODEWRITER TODO - Implement any shared variables (queues etc. here)
        # each entry is a tuple (args,kwargs) (list,dict)
        # to be added to process args/kwargs and key is process_name
        
        print('Get args from child!! :)')
        
        return {}



if __name__ == "__main__":

    #sys.path.append('/mnt/c/shared/simple')
    #mod_simple = importlib.import_module('simple')
    
    #name_to_process['simple'] = mod_simple.__process_entry__
  
    s = TestServerControl('servers/laptop', name_to_process)
    s.main()
