import requests
import json

import pprint
import time


import argparse
import getpass
import sys
import os

import urllib3
urllib3.disable_warnings()


try:
    import keyring
except ModuleNotFoundError:
    pass

ROTATING_CURSOR = '|/-\\|/-\\'


# Print iterations progress
def print_progress(percentage, finished=False, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(  percentage )
    filled_length = int(round(bar_length * (percentage/100.0)))
    bar = '#' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s    ' % (prefix, bar, percents, '%', suffix)),

    if finished:
        sys.stdout.write('\n')
    sys.stdout.flush()

class AssignKeyValue(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value


class EnvDefault(argparse.Action):
    def __init__(self, envvar, required=True, default=None, **kwargs):
        if not default and envvar:
            if envvar in os.environ:
                default = os.environ[envvar]
        if required and default:
            required = False
        super(EnvDefault, self).__init__(default=default, required=required, 
                                         **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values)


class Password(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if values is None:
            values = getpass.getpass()

        setattr(namespace, self.dest, values)



def make_parser():
    parser = argparse.ArgumentParser(description='Submit python script to the spark cluster')
    parser.prog = 'livy_submit'


    parser.add_argument( "--livy-url", action=EnvDefault, envvar='LIVY_SUBMIT_URL', 
                    help="Specify the LIVY URL to process (Can also be specifed by setting the LIVY_SUBMIT_URL environment variable)")


    group0_b = parser.add_argument_group('Livy Settings', 'Livy specific settings')

    group0_b.add_argument('-u', '--username', action='store' , dest='username',
                    help='The username to use when executing on the cluster. If not provided, defaults to the current username running the livy_submit script')

    group0_b.add_argument('-p', '--password', action=Password, nargs='?', dest='password',
                    help='The password used when connecting to livy. If not provided on the commandline, you will be asked to type the password (hidden)')


    group00 = parser.add_argument_group('Workflow settings', 'Determines what you want to do')
    group0 = group00.add_mutually_exclusive_group(required=True)

    group0.add_argument('-s' , '--submit', action='store' , type=argparse.FileType('r'), metavar='PYTHON_SCRIPT', 
                    help='Python script that you want to submit to the cluster')

    group0.add_argument('-l', '--list-sessions', action='store_true' , dest='list_sessions',
                    help='Get list of all running sessions')

    group0.add_argument('-i', '--information', type=int, dest='id_information', metavar=('SESSION_ID'), 
                    help='Get information about a currently running session')

    group0.add_argument('-d', '--delete', type=int, dest='id_delete', metavar=('SESSION_ID'),
                    help='Delete a currently running session')

    group0.add_argument('-t', '--task-status', type=int, nargs=2, dest='statement_information', metavar=('SESSION_ID' , 'STATEMENT_ID'), 
                    help='Display progress for given statement in given session')

    group0.add_argument('-q', '--retrieve-statement', type=int, nargs=2, dest='retrieve_statement', metavar=('SESSION_ID' , 'STATEMENT_ID'), 
                    help='Display the the contents of a given statement')

    group0.add_argument('-r', '--retrieve-statement-output', type=int, nargs=2, dest='retrieve_statement_output', metavar=('SESSION_ID' , 'STATEMENT_ID'), 
                    help='Display the output for the given statement in the given session')



    group1 = parser.add_argument_group('Submit settings', 'Settings that deal with the different items regarding the submission of a python script')

    group1.add_argument('--files', dest='files', metavar='files', nargs='+', default=[],
                        help='Files to be placed in executor working directory')

    group1.add_argument('--py-files', dest='py_files', metavar='python_files', nargs='+', default=[],
                    help='Files to be placed on the PYTHONPATH')

    group1.add_argument('-n', '--nowait', action='store_true' , dest='nowait' ,
                    help='Do not wait for the execution of the program to finish on the spark cluster')

    group1.add_argument('-k', '--keep-session-alive', action='store_true' ,dest='keep_session_alive' , 
                    help='Do not delete the session after the execution is finished')

    group1.add_argument('-c', '--connect-existing-session', type=int, dest='connect_existing_session' , metavar='SESSION_ID',
                    help='Submit the python script to the exsting session (i.e. don\'t start a new session). This option also automatically sets the keep-alive option')

    group1.add_argument('-w', '--write-session-id', dest='filename_session_id' , metavar='SESSION_ID_FILE',
                    help='Write the session id used/created for this task to the given file')

    group1.add_argument('--task-name' , dest='task_name', metavar='TASK_NAME',
                    help='Specify the name to be used for this spark job. Defaults to the filename of the script you are submitting')


    group1.add_argument('-o', '--output'  , dest='output_file', metavar='OUTPUT_FILE',
                    help='Write the output of the driver to the given file')


    group2 = parser.add_argument_group('Driver settings', 'All options related to driver settings')

    group2.add_argument('--driver-cores' , dest='driver_cores', type=int, metavar='DRIVER_CORES', default=4,
                    help='Specify the number of cores you want to use for the driver (Default: 4)')
    group2.add_argument('--driver-memory' , dest='driver_memory', metavar='DRIVER_MEMORY', default='32g',
                    help='Specify the memory for the driver (Default: 32g)')



    group3 = parser.add_argument_group('Executor settings', 'All options related to executor settings')

    group3.add_argument('--exe-env', dest='spark_executor_var', metavar='EXECUTOR_VAR',
                        help='key-value pair for spark executor environment',
                        nargs='*', action=AssignKeyValue, default=None)
    group3.add_argument('--num-executors' , dest='num_executors', type=int, metavar='NUM_EXECUTORS', default=10,
                    help='Specify the number of executors you want to use in non dynamic setting (Default: 10)')
    group3.add_argument('--executor-cores' ,  dest='executor_cores', type=int, metavar='EXECUTOR_CORES', default=4,
                    help='Specify the number of cores you want to use for each executor (Default: 4)')
    group3.add_argument('--executor-memory' ,  dest='executor_memory', metavar='EXECUTOR_MEMORY', default='32g',
                    help='Specify the memory for each executor (Default: 32g)')
    group3.add_argument('--dynamic-max-executors' ,  dest='dynamic_max_executors', metavar='NUM_EXECUTORS', default='50',
                    help='Specify the maximum number of dynamic executors (Default: 50)')
    group3.add_argument('--spark-yarn-executor-memoryoverhead' ,  dest='spark_yarn_executor_memoryoverhead', metavar='MEMORY',
                    help='Specify the spark.yarn.executor.memoryOverhead value in megabytes')

    return parser


def session_list(parsed_arguments):
    sessions = get_sessions( parsed_arguments) 
    
    num_sessions = len( sessions.get('sessions' , []))

    if num_sessions == 0:
        print("Currently no active livy sessions")
    else:
        print_format = '{:<5} {:<15} {:<10}'

        print( print_format.format( 'ID' , 'USER' , 'STATE' ))
        print('------------------------------')
        for s in sessions.get('sessions'):
            print( print_format.format( s['id'] , '' if s['proxyUser'] is None else s['proxyUser'] , s['state'] ))
    print()            
    

def get_password( parsed_arguments ):
    if parsed_arguments['password'] is not None:
        return parsed_arguments['password']
    else:
        return keyring.get_password( 'livysubmit' , parsed_arguments['username'])


def get_statement(parsed_arguments, session_id, statement_id):
    result = requests.get( '{}/sessions/{}/statements/{}'.format(parsed_arguments['livy_url'], session_id, statement_id)
                            , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                            , headers=parsed_arguments['headers'], verify=False)
    return result.json()



def get_session(parsed_arguments, session_id):
    found_session = False

    sessions = get_sessions( parsed_arguments) 
    for s in sessions.get('sessions'):
        if s['id'] == session_id:
            found_session = True


    if found_session:
        result = requests.get( '{}/sessions/{}'.format(parsed_arguments['livy_url'], session_id)
                                , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                                , headers=parsed_arguments['headers'], verify=False)

        result_object = result.json()

        result_statements = requests.get( '{}/sessions/{}/statements'.format(parsed_arguments['livy_url'], session_id)
                                , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                                , headers=parsed_arguments['headers'], verify=False)

        if result_statements.status_code == 200:
            result_statements_object = result_statements.json()
            result_object['statements'] = result_statements_object
        else:
            result_object['statements'] = { 'statements' : [] }

        return result_object

    else:
        print("Cannot get session information for session {} because it does not exist".format(session_id))
        return None



def get_sessions(parsed_arguments):
    result = requests.get( '{}/sessions'.format(parsed_arguments['livy_url'])
                            , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                            , headers=parsed_arguments['headers'], verify=False)
    return result.json()


def print_statement(parsed_arguments):
    session_id, statement_id = parsed_arguments['retrieve_statement']
    statement_status = get_statement(parsed_arguments, session_id, statement_id )



    if 'code' in statement_status.keys(): 
        # Check if there was an error or whether it was successful
        if parsed_arguments['output_file'] is not None:
            print()
            print('Storing code of statement {} of session {} in file {}'.format( statement_id, session_id, parsed_arguments['output_file'] ))
            with open( parsed_arguments['output_file'], 'w') as f:
                f.write(statement_status['code'])
        else:
            print(statement_status['code'])
    else:
        print('ERROR: no code in statement {} of session {}'.format(statement_id, session_id))


def print_statement_output(parsed_arguments):
    session_id, statement_id = parsed_arguments['retrieve_statement_output']
    statement_status = get_statement(parsed_arguments, session_id, statement_id )



    if statement_status['output'] is not None:
        # Check if there was an error or whether it was successful

        if statement_status['output']['status'] == 'error':
            print()
            print( 'ERROR {} while executing application on the driver:'.format( statement_status['output']['ename']))
            print()
            print( statement_status['output']['evalue'])
            print()
            print("Traceback:")
            print( ''.join( statement_status['output']['traceback'] ) )
            
        elif statement_status['output']['status'] == 'ok':

            if parsed_arguments['output_file'] is not None:
                print()
                print('Storing output in file {}'.format( parsed_arguments['output_file'] ))
                with open( parsed_arguments['output_file'], 'w') as f:
                    for i in statement_status['output'].get('data', {}).keys():
                        f.write( statement_status['output']['data'][i])
                        f.write('\n')
            else:

                print()
                print( 'Output of the application on the driver:')
                for i in statement_status['output'].get('data', {}).keys():
                    print("--------------- {} ---------------".format(i))
                    print( statement_status['output']['data'][i])
        else:
            print("Unknown output status...")



def print_statement_progress(parsed_arguments):
    session_id, statement_id = parsed_arguments['statement_information']

    print("Statement progress for statement {} in session {}".format( statement_id, session_id))

    for i in range(17200):
        time.sleep(1)
        statement_status = get_statement(parsed_arguments, session_id, statement_id )
        print_progress( 100*statement_status['progress'], prefix=statement_status['state'], suffix='Complete' , bar_length=50)
        if statement_status['state'] not in [ 'running', 'waiting'] :
            break

    print_progress( 100*statement_status['progress'],finished=True, prefix=statement_status['state'], suffix='Complete' , bar_length=50)



def session_information(parsed_arguments):
    result = get_session(parsed_arguments , parsed_arguments['id_information'])
    if result is not None: 
        print("Information for session {}:".format(parsed_arguments['id_information']))
        print("    User : {}".format( result['proxyUser']))
        print("    Kind : {}".format( result['kind']))
        print("    State: {}".format( result['state']))


        print()
        print('Statements:')
        for s in result['statements']['statements']:
            if s['state'] == 'running':
                print('    Statement {} (state: running, progress = {:.02f}%)'.format( s['id'], 100* s['progress'] ) )
            else:
                print('    Statement {} (state: {})'.format( s['id'],  s['state'] ) )




        print()
        print()
        print("log:")
        for i in result['log']:
            print(i)
        

def session_delete(parsed_arguments, id_to_delete=None):
    if id_to_delete is None:
        id_to_delete = parsed_arguments['id_delete']

    found_session = False

    sessions = get_sessions( parsed_arguments) 
    for s in sessions.get('sessions'):
        if s['id'] == id_to_delete:
            found_session = True

   
    if found_session: 
        result = requests.delete( '{}/sessions/{}'.format(parsed_arguments['livy_url'], id_to_delete)
                                , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                                , headers=parsed_arguments['headers'], verify=False)
        
        pprint.pprint(result.json()) 

    else:
        print('Cannot delete session {} because it does not exist'.format( id_to_delete))




def submit_script(parsed_arguments):

    if parsed_arguments['connect_existing_session'] is None:
        data = {
            'proxyUser' : parsed_arguments['username']
            , 'name' : parsed_arguments['task_name']
            , 'kind' : 'pyspark'
            , 'numExecutors' : parsed_arguments['num_executors']
            , 'executorCores' : parsed_arguments['executor_cores']
            , 'executorMemory' : parsed_arguments['executor_memory']
            , 'driverCores' : parsed_arguments['driver_cores']
            , 'driverMemory' : parsed_arguments['driver_memory']
            , 'conf': {
                     'spark.dynamicAllocation.enabled' :'true'
                    ,'spark.dynamicAllocation.maxExecutors' : parsed_arguments['dynamic_max_executors'] 
                    ,'spark.dynamicAllocation.minExecutors' : 6
                    ,'spark.shuffle.service.enabled' : 'true'
                }
        }

        if parsed_arguments['spark_executor_var']:
            exe_env = {'spark.executorEnv.{}'.format(key): value for key, value in parsed_arguments['spark_executor_var'].items()}
            data['conf'].update(exe_env)
        if parsed_arguments['spark_yarn_executor_memoryoverhead']:
            data['conf']['spark.yarn.executor.memoryOverhead'] = parsed_arguments['spark_yarn_executor_memoryoverhead']
        if parsed_arguments['py_files']:
            data['pyFiles'] = parsed_arguments['py_files']
        if parsed_arguments['files']:
            data['files'] = parsed_arguments['files']


        # Create the session
        session_result = requests.post( '{}/sessions'.format(parsed_arguments['livy_url'])
                                , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                                , headers=parsed_arguments['headers'], verify=False, data=json.dumps(data))

        session_id = session_result.json()['id']

        print("Started session with id = {}".format(session_id))

        sys.stdout.write("Waiting for session to become idle before sending statements |")

        for i in range(3600):
            time.sleep(1)
            sys.stdout.write("\rWaiting for session to become idle before sending statements {}".format( ROTATING_CURSOR[ i% len(ROTATING_CURSOR) ] ))
            sys.stdout.flush()

            session = get_session( parsed_arguments , session_id )
            if session['state'] == 'idle':
                sys.stdout.write("\rWaiting for session to become idle before sending statements DONE")
                sys.stdout.write('\n')
                sys.stdout.flush()
                break 


            if session['state'] == 'dead':
                sys.stdout.write("\rWaiting for session to become idle before sending statements DONE")
                sys.stdout.write('\n')
                sys.stdout.flush()
                print()
                print("Session ended up in DEAD state, cleaning up stale session")
                session_delete( parsed_arguments, session_id) 
                sys.exit(1)
                break 

    else:
        # Connect to existing session
        result = get_session(parsed_arguments , parsed_arguments['connect_existing_session'])
        if result is None: 
            print("Cannot connect to session {} because it does not exist".format( parsed_arguments['connect_existing_session'] ))
            sys.exit(1)
        else:
            print("Connecting to existing session {}".format( parsed_arguments['connect_existing_session'] ))
            session_id = parsed_arguments['connect_existing_session']  
        

    file_contents = parsed_arguments['submit'].read()
    data_code = {
        'code' : file_contents 
    }
    statement_result = requests.post( '{}/sessions/{}/statements'.format(parsed_arguments['livy_url'], session_id)
                            , auth=(parsed_arguments['username'], get_password( parsed_arguments ))
                            , headers=parsed_arguments['headers'], verify=False, data=json.dumps(data_code))

    statement_id = statement_result.json()['id']

    print("Now executing the contents of the script {} (statement id={})".format( parsed_arguments['task_name'], statement_id ))
    for i in range(17200):
        time.sleep(1)
        statement_status = get_statement(parsed_arguments, session_id, statement_id )
        #print()        
        #print( 'State   : {}'.format(statement_status['state'] ))
        #print( 'Progress: {}'.format(statement_status['progress'] ))
        print_progress( 100*statement_status['progress'], prefix=statement_status['state'], suffix='Complete' , bar_length=50)

        if statement_status['state'] not in [ 'running', 'waiting'] :
            break

    print_progress( 100*statement_status['progress'],finished=True, prefix=statement_status['state'], suffix='Complete' , bar_length=50)

    if statement_status['output'] is not None:
        # Check if there was an error or whether it was successful

        if statement_status['output']['status'] == 'error':
            print()
            print( 'ERROR {} while executing application on the driver:'.format( statement_status['output']['ename']))
            print()
            print( statement_status['output']['evalue'])
            print()
            print("Traceback:")
            print( ''.join( statement_status['output']['traceback'] ) )
            
        elif statement_status['output']['status'] == 'ok':

            if parsed_arguments['output_file'] is not None:
                print()
                print('Storing output in file {}'.format( parsed_arguments['output_file'] ))
                with open( parsed_arguments['output_file'], 'w') as f:
                    for i in statement_status['output'].get('data', {}).keys():
                        f.write( statement_status['output']['data'][i])
                        f.write('\n')
            else:

                print()
                print( 'Output of the application on the driver:')
                for i in statement_status['output'].get('data', {}).keys():
                    print("--------------- {} ---------------".format(i))
                    print( statement_status['output']['data'][i])



        else:
            print("Unknown output status...")


    if not parsed_arguments['filename_session_id'] is None:
        with open( parsed_arguments['filename_session_id'], 'w' ) as f:
            f.write( str(session_id) )

    if not parsed_arguments['keep_session_alive']:
        print()
        print("Finished executing script, now removing the spark session")
        session_delete( parsed_arguments, session_id) 


def parse_arguments( args ):
    args_dict = vars( args )


    if args_dict['username'] is None:
        if os.environ.get( 'USERNAME', '') == '':
            if os.environ.get('USER', '') == '':
                raise Exception("Cannot determine the username")
            else:
                args_dict['username'] = os.environ.get('USER')
        else:
            args_dict['username'] = os.environ.get('USERNAME')
    
    if args_dict['submit'] is not None:
        if args_dict['task_name'] is None:
            args_dict['task_name']  = os.path.basename( args_dict['submit'].name )

        args_dict['task_name'] = 'LivySubmit - ' + args_dict['task_name']

    



    if args_dict['connect_existing_session'] is not None:
        args_dict[ 'keep_session_alive'] = True


    args_dict['headers'] = {'Content-Type': 'application/json' , 'X-Requested-By' : args_dict['username'] }

    if args_dict['username'] is None:
        _password = keyring.get_password( 'livysubmit' , args_dict['username'] )
        if _password is None:
            raise Exception("No password stored for user {}. Please use command \"keyring set livysubmit {}\" to store password".format( args_dict['username'], args_dict['username']) )


    return args_dict
          

def main():
    parser = make_parser();
    args = parser.parse_args()

    parsed_arguments = parse_arguments(args)


    if 'keyring' not in sys.modules:
        if parsed_arguments['password'] is None:
            print('ERROR: keyring library is not available and you did not provide a password')
            sys.exit(1)

    if parsed_arguments['list_sessions']:
        session_list( parsed_arguments )

    elif parsed_arguments['id_information'] is not None:
        session_information( parsed_arguments )

    elif parsed_arguments['id_delete'] is not None:
        session_delete( parsed_arguments )

    elif parsed_arguments['statement_information'] is not None:
        print_statement_progress( parsed_arguments )


    elif parsed_arguments['retrieve_statement'] is not None:
        print_statement( parsed_arguments )

    elif parsed_arguments['retrieve_statement_output'] is not None:
        print_statement_output( parsed_arguments )
    
    elif parsed_arguments['submit'] is not None:
        submit_script( parsed_arguments )
