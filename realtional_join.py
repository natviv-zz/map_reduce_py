import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    #print key, value
    #words = value.split()
    #for w in words:
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    value = []
    for v in list_of_values:
      if v[0] == 'order' and len(value) == 0: 
        value.extend(v)
    #print value    
    for v in list_of_values:    
      if v[0] == 'line_item':
        f_val = list(value)
        #print "length val " +str(len(value))
        f_val.extend(v)
        #print "length f_val "+str(len(f_val))
        mr.emit(f_val)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
