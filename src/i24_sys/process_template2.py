
# TEMPLATE FOR A DYNAMICALLY MANAGED PROCESS

# This process/module is loaded dynamically and executed;
# no reference/inheritance of the ServerControl is needed,
# only the implementation of the entry function and the optional setup function.
# The mudule is loaded by configuring the path and name of the module itself


# Optional setup function called at the initialization phase; 
# optional parameters/arguments can be provided for calling the entry function 
# The function name SHOULD be the exactly as here, without parameters       
def __process_setup__():

    return None
        
# Required entry point for executing the module;
# parameters/arguments are supplied by the ServerControl based on the configuration
# The function name SHOUL be exactly as here, parameters are defined by the user and SHULD match with the configuration
def __process_entry__(arg1, index=None):
    
    # invoke something which do the actual work..
    p = simple_process.SimpleProcess(index)
    
    # override the signal handlers as soon as possible
    signal.signal(signal.SIGINT, p.sigint_handler)
    signal.signal(signal.SIGUSR1, p.sigusr_handler)
    
    # do the work / main loop
    p.main()