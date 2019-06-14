import sys
from pyspark import SparkContext

sc = SparkContext(appName="inf553")
input_A = sys.argv[1]
input_B = sys.argv[2]
output_file = sys.argv[3]

rdd1 = sc.textFile(input_A)
rdd2 = sc.textFile(input_B)

def apple(input):
    l = []
    for i in range(1,4):
        key = (input[0][1],str(i))
        value = ('A', input[0][3],input[1])
        l.append((key,value))
    return l


def bapple(input):
    l = []
    for j in range(1,4):
        key = (str(j), input[0][3])
        value = ('B', input[0][1],input[1])
        l.append((key,value))
    return l

a = rdd1.map(lambda x:x.split(',[')).map(lambda x : (x[0],'[' +x[1])).flatMap(apple)
b = rdd2.map(lambda x:x.split(',[')).map(lambda x: (x[0],'[' +x[1])).flatMap(bapple)


mapper = a.join(b)

def func(x):
    l1 = [None, None]
    l1[0] = x[0]
    l2 = []
    for i in x[1]:
        for j in i:
            l2.append(j)
    l1[1] = l2
    return tuple(l1)

def helper(v):
    arr = [(int(v[0][0]), int(v[0][1])), []]
    array = []
    j = v[1]
    A = eval(j[0][2])
    B = eval(j[1][2])
    for k in A:
        for l in B:
            if k[1] == l[0]:
                array.append([k[0],l[1], k[2]*l[2]])
    arr[1] += array
    return arr

ans = mapper.filter(lambda (x, (y, z)): y[0] != z[0] and y[1] == z[1]).map(helper).groupByKey().map(lambda x: (x[0],list(x[1]))).map(func).sortByKey().collect()

reducer = []
for sub_matrix in ans:
    
    sub_matrix_temp = sc.parallelize(sub_matrix[1])
    sub_matrix_map = sub_matrix_temp.map(lambda x: ((x[0],x[1]),x[2]))
    sub_matrix_reduce = sub_matrix_map.reduceByKey(lambda x,y: x+y).sortByKey().map(lambda ((x, y), z): [x, y, z]).collect()
    final_ans.append([sub_matrix[0], sub_matrix_reduce])
    
        
with open(output_file,'w') as f:
  for i in reducer:
    s = str(i)
    f.write(s[1:-1])

sc.stop()