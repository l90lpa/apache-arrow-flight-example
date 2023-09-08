import pyarrow as pa
import pyarrow.flight
import numpy as np

client = pa.flight.connect("grpc://0.0.0.0:8815")

my_variable = np.array([[1,2,3],[4,5,6]], dtype=np.float64)

# Upload a new dataset
pa_array = pa.FixedShapeTensorArray.from_numpy_ndarray(np.expand_dims(my_variable, axis=0))

data_table = pa.table(
    [pa_array],
    names=["my_variable"]
)

upload_descriptor = pa.flight.FlightDescriptor.for_path("variable/my_variable")
writer, _ = client.do_put(upload_descriptor, data_table.schema)
writer.write_table(data_table)
writer.close()

# # Retrieve metadata of newly uploaded dataset
flight = client.get_flight_info(upload_descriptor)
descriptor = flight.descriptor
print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
print("=== Schema ===")
print(flight.schema)
print("==============")

# Read content of the dataset
reader = client.do_get(flight.endpoints[0].ticket)
read_table = reader.read_all()
value_arr = pa.FixedShapeTensorArray.from_storage(read_table.field(0).type, read_table.column(0).combine_chunks().storage).to_numpy_ndarray()
count = np.size(value_arr,axis=0)
assert(count == 1)
value = np.squeeze(value_arr, axis=0)
print(value)