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
    key1 = record[0]
    key2 = record[1]
    value = record
    #print key, value
    #words = value.split()
    #for w in words:
    mr.emit_intermediate(key1, key2)
    mr.emit_intermediate(key2, key1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #for v in list_of_values:
      #if v[0] == 'order' and len(value) == 0: 
       # value.extend(v)
    #print value    
    print key
    for v in list_of_values:    
        #print v
        mod_list = list(list_of_values)
        #print len(list_of_values)
        #print len(mod_list)
        mod_list.remove(v)
        #print len(mod_list)
        #print v
        #print mod_list
        if v not in mod_list:
          print "HI"
          mr.emit((key,v)) 

        #f_val = list(value)
        #print "length val " +str(len(value))
        #f_val.extend(v)
        #print "length f_val "+str(len(f_val))
    #mr.emit(key, count)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
