import MapReduce
import sys
mr = MapReduce.MapReduce()

def mapper(record):
    #key: person
    #value: friend
    key = record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: person
    # value: list of ones
    count = 0
    for value in list_of_values:
        count += value
    mr.emit((key, count))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)