
import simple_process
import signal
        
def __process_setup__():

    return None
        

def __process_entry__(arg1, index=None):
    
    p = simple_process.SimpleProcess(index)
    
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    p.main()