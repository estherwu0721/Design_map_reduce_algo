import MapReduce
import sys

mr = MapReduce.MapReduce()

# We assume that matrix dimensions is known beforehand
# A  is MxK matrix, whereas B is KxN one
M = 5
K = 5
N = 5


def mapper(record):
    #key = matrix ID
    #value = row, col, value
    key = record[0]
    row = record[1]
    col = record[2]
    value = record[3]
    if key == "a":
        for i in range(5):
            mr.emit_intermediate((row,i),(col,value))
    if key == "b":
        for j in range(5):
            mr.emit_intermediate((j,col),(row,value))


def reducer(key, list_of_values):
    # key: destination cell position
    # values: values to be summed
    total = 0
    value1 = {}
    value2 = {}
    for v in list_of_values:
        if v[0] in value1:
            value2[v[0]] = v[1]
        else:
            value1[v[0]] = v[1]
    for k in value2.keys():
        total = total + value2[k] * value1[k]
    mr.emit((key[0],key[1],total))


inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
