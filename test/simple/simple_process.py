import os
import time
import sys

class SimpleProcess:

    def __init__(self, index=0):
        self.index = index
        print('SimpleProcess {}: created (PID={})'.format(self.index, os.getpid()))
        self.run = True      
        
    def sigusr_handler(self, sig, frame):
        print('SimpleProcess {}: SIGUSR received'.format(self.index))
        
        self.run = False;
        
        
    def sigint_handler(self, sig, frame):
        print('SimpleProcess {}: SIGINT received'.format(self.index))
        
        # nothing to cleanup so we can make an immediate exit
        print('SimpleProcess {}: exit'.format(self.index))
        sys.exit(2)
        
        
    def main(self):
    
        print('SimpleProcess {}: main'.format(self.index))
    
        while self.run:
            print('Simple process {}...'.format(self.index))
            time.sleep(5)  

        print('SimpleProcess {}: graceful exit'.format(self.index)) 
        