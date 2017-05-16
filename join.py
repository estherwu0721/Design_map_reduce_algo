import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # key: order ID
    # value: rest of the record
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: order ID
    # value: records with the same order ID
    lines = []
    order = []
    for value in list_of_values:
        if value[0] == "order":
            order = value
        else:
            lines.append(value)

    for line in lines:
        mr.emit(order + line)


inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
