import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    #key: trimmed sequence
    #value: occurances of each trimmed sequence
    key = record[1][:-10]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    #key: trimmed sequence
    #value: list of ones
    mr.emit(key)

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)