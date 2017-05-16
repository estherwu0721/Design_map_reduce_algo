import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    #key = document ID
    #value = document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for word in words:
        mr.emit_intermediate(word, key)

def reducer(key, list_of_values):
    # key: word
    # value: document ID list
    docIDs = []
    for ID in list_of_values:
        if ID not in docIDs:
            docIDs.append(ID)
    mr.emit((key, docIDs))


inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
