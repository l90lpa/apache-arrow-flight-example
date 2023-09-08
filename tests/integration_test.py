import pytest
from xprocess import ProcessStarter
import pyarrow as pa
import pyarrow.flight
import numpy as np
from my_rpc.client import *
import os
# import data_transform_server as dts

@pytest.fixture
def fixture_server(xprocess):
    class Starter(ProcessStarter):
        # xprocess will now attempt to clean up for you upon interruptions
        terminate_on_interrupt = True

        # passing a custom enviroment so that the my_rpc module is in the path in the new process 
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = "/home/lpada/repos/temp/aaf"
        env = new_env

        # startup pattern, xprocess will check this pattern to determine if the server has started
        pattern = "Server has started"

        # command to start the server
        args = ['python3', '/home/lpada/repos/temp/aaf/tests/server_program.py']

    # ensure process is running and return its logfile
    logfile = xprocess.ensure("fixture_server", Starter, restart=True, persist_logs=False)
    
    yield RPCContext()

    # clean up whole process tree afterwards
    xprocess.getinfo("fixture_server").terminate()

def test_create_and_read(fixture_server):
    context = fixture_server

    data0 = np.array([[1,2,3],[4,5,6]], dtype=np.float64)
    remote_data_handle = create_rpcvariable(context, data0)
    data1 = read_rpcvariable(context, remote_data_handle)
    assert np.array_equal(data0, data1)

def test_create_update_and_read(fixture_server):
    context = fixture_server

    data0 = np.array([[1,2,3],[4,5,6]], dtype=np.float64)
    remote_data_handle = create_rpcvariable(context, data0)
    data1 = 2.0 * data0
    update_rpcvariable(context, remote_data_handle, data1)
    data2 = read_rpcvariable(context, remote_data_handle)
    
    assert np.array_equal(data1, data2)

def test_create_and_delete(fixture_server):
    context = fixture_server

    data0 = np.array([[1,2,3],[4,5,6]], dtype=np.float64)
    remote_data_handle = create_rpcvariable(context, data0)
    delete_rpcvariable(context, remote_data_handle)
    