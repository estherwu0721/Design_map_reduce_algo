import MapReduce
import sys

mr = MapReduce.MapReduce()


def mapper(record):
    #key: record (friends_pair)
    #value: occurances of friends pair
    friends_pair = tuple(sorted(record)) #remove order of the person and friend
    mr.emit_intermediate(friends_pair, 1)


def reducer(friends_pair, list_of_values):
    #key: friends_pair
    #value: list of one(s)
    if len(list_of_values) == 1:
        mr.emit((friends_pair[0], friends_pair[1]))
        mr.emit((friends_pair[1], friends_pair[0]))

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)