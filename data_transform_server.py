from pathlib import Path

import pyarrow as pa
import pyarrow.flight as fl
import numpy as np

variables = {}

# Define a Flight service that performs data transformations
class DataTransformService(fl.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815",
                repo=Path("./datasets"), **kwargs):
        super(DataTransformService, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo

    def _make_flight_info(self, dataset_path):

        record = variables[dataset_path]
        num_rows = 1
        schema = record["schema"]
        total_bytes = record["total_bytes"]

        descriptor = pa.flight.FlightDescriptor.for_path(
            dataset_path.encode('utf-8')
        )
        endpoints = [pa.flight.FlightEndpoint(dataset_path, [self._location])]

        return pa.flight.FlightInfo(schema,
                                    descriptor,
                                    endpoints,
                                    num_rows,
                                    total_bytes)

    def list_flights(self, context, criteria):
        for variable_name in variables:
            yield self._make_flight_info(variable_name)

    def get_flight_info(self, context, descriptor):
        return self._make_flight_info(descriptor.path[0].decode('utf-8'))

    def do_put(self, context, descriptor, reader, writer):
        dataset_path = descriptor.path[0].decode('utf-8')
        print(dataset_path)
        data_table = reader.read_all()
        value_arr = pa.FixedShapeTensorArray.from_storage(data_table.field(0).type, data_table.column(0).combine_chunks().storage).to_numpy_ndarray()
        count = np.size(value_arr,axis=0)
        assert(count == 1)
        value = np.squeeze(value_arr, axis=0)
        print(value)
        variables[dataset_path] = {"value": value, "schema": data_table.schema, "total_bytes": data_table.get_total_buffer_size()}

    def do_get(self, context, ticket):
        dataset_path = ticket.ticket.decode('utf-8')
        value = variables[dataset_path]["value"]
        data_table = pa.table(
            [pa.FixedShapeTensorArray.from_numpy_ndarray(np.expand_dims(value, axis=0))],
            names=["my_variable"]
        )
        return pa.flight.RecordBatchStream(data_table)
    
if __name__ == '__main__':
    server = DataTransformService()
    server._repo.mkdir(exist_ok=True)
    server.serve()


