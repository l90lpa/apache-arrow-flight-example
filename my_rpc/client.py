import pyarrow as pa
import pyarrow.flight
import numpy as np
import uuid

class RPCContext:
    def __init__(self):
        self.client = pa.flight.connect("grpc://0.0.0.0:8815")

    # def __del__(self):
    #     print('Destructor called')

class RPCVariable:
    def __init__(self, uuid):
        self.uuid = uuid

    # def __del__(self):
    #     print('Destructor called')

def create_rpcvariable(context: RPCContext, value: np.ndarray) -> RPCVariable:

    # Get new uuid
    new_uuid = uuid.uuid4()
    new_variable = RPCVariable(new_uuid)

    update_rpcvariable(context, new_variable, value)

    return new_variable

def read_rpcvariable(context: RPCContext, variable: RPCVariable) -> np.ndarray:

    reader = context.client.do_get(pa.flight.Ticket(str(variable.uuid)))
    read_table = reader.read_all()
    array_element_type = read_table.field(0).type
    array_storage = read_table.column(0).combine_chunks().storage
    value_array = pa.FixedShapeTensorArray.from_storage(array_element_type, array_storage).to_numpy_ndarray()
    count = np.size(value_array,axis=0)
    assert(count == 1)
    value = np.squeeze(value_array, axis=0)

    return value

def update_rpcvariable(context: RPCContext, variable: RPCVariable, value: np.ndarray) -> RPCVariable:
    
    # Put the value in a Table
    data_table = pa.table(
        [pa.FixedShapeTensorArray.from_numpy_ndarray(np.expand_dims(value, axis=0))],
        names=[str(variable.uuid)]
    )

    # Send the value
    upload_descriptor = pa.flight.FlightDescriptor.for_path(str(variable.uuid))
    writer, _ = context.client.do_put(upload_descriptor, data_table.schema)
    writer.write_table(data_table)
    writer.close()

    return variable