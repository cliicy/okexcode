# ab,c,,d
# a,b,c,d,e
import mmap, os, time
f = open("D:\\data\\ceshimmap.txt", "r+")
VDATA = mmap.mmap(f.fileno(), 0)
xx = VDATA.read()
size = len(VDATA)
print(size)
print(xx)
print('before: %s' % VDATA.tell())
VDATA.move(1, 2, 0)
print('after: %s' % VDATA.tell())



# start = 11
# end = size
# length = end - start
# newsize = size - length
# VDATA.move(start, end, size - end)
# VDATA.flush()
# print(len(VDATA))
# VDATA.close()
# print(newsize)
# f.truncate(newsize)
f.close()
