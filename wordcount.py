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
    mat = record[0]
    row = record[1]
    col = record[2]
    val = record[3]
    #print key, value
    #words = value.split()
    #for w in words:
    if mat == 'a':
      for k in range(0,5):
        mr.emit_intermediate((row,k), (col,val))
    if mat == 'b':
      for k in range(0,5):
        mr.emit_intermediate((k,col),(row,val))    
    #mr.emit_intermediate(key2, key1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #for v in list_of_values:
      #if v[0] == 'order' and len(value) == 0: 
       # value.extend(v)
    #print value    
    #print key
    #print len(list_of_values)
    #print key 
    #print list_of_values
    sum = 0;
    for v in list_of_values:
        mod_list = list(list_of_values)
        mod_list.remove(v)
        for w in mod_list:
          if v[0]==w[0]:
            sum= sum + v[1]*w[1]

    #string_set = set(list_of_values)
    #print len(string_set)
    mr.emit((key[0],key[1],sum/2))
    #for v in list:    
     #   mr.emit(v)
        #print v
        #mod_ = list(list_of_values)
        #print len(list_of_values)
        #print len(mod_list)
        #mod_list.remove(v)
        #print len(mod_list)
        #print v
        #print mod_list
        #if v not in mod_list:
          #print "HI"
          #mr.emit((key,v)) 

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
