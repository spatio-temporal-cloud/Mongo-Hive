import sys, os, stat
import sched
import types, time
import re



dest = "/home/zengmingyu/data"
count = 0
s = sched.scheduler(time.time, time.sleep)

#hive_sql='select count(*) from ssp_factbids'
load_prefix = "load data local inpath " 
load_midfix =  " overwrite into table "
load_suffix = " partition (day=\""
startTime = "2013"
endTime = "2013"
day = time.strftime('%Y%m%d', time.localtime(time.time()))




def count_new(path):
   count = 0
   for item in os.listdir(path):
#print item
      item = os.path.join(path, item)
      if os.path.isfile(item) and os.path.splitext(item)[1] == '.new':
#         print get_table(item)
         count += 1

   return count

def get_table(filePath):
   fileName =  os.path.splitext(os.path.basename(filePath))[0]
   return fileName.split('_')[2]

def synchronization_finished(path):

   start = time.time()
   time.sleep(2)
   while 1:
      if count_new(path) - count == 8:
         return  True
      else :
         print False
      if time.time() - start >= 3600:
         return False
      time.sleep(1)

def load_data(path):
   loadhql = open("loaddata.hql", "w")
   loadhql.write("use maize;\n")
   for item in os.listdir(path):
      match =  re.match(r'\w+\_\w+\_\w+\.new', item) 
      if match:
         item= os.path.join(path, item)
         if os.path.isfile(item) and os.path.splitext(item)[1] == '.new':
            table = get_table(item) 
            if table == 'orders' or table == 'customers' or table == 'visits':
               continue
#print table
#            print os.path.basename(item)
            hiveql = load_prefix + '\'' + item + '\'' + load_midfix + table + load_suffix + day + "\");\n"
            loadhql.write(hiveql)
#            print hiveql
   loadhql.close()
   ret = os.system("hive -f loaddata.hql")
   loadhql = os.path.join(os.getcwd(), "loaddata.hql")
   os.remove(loadhql)
   if ret != 0:
      print "load data to hive failed"
   for item in os.listdir(path):
      match =  re.match(r'\w+\_\w+\_\w+\.new', item) 
      if match:
         item = os.path.join(path, item)
         old = os.path.splitext(item)[0] + '.old'
         os.rename(item, old)
       
  
def join_tables():
   print "joinning tables"

def event_func():
#   endTime = time.time()
   print time.time()
   global count, day, startTime, endTime
   startTime = time.strftime("%Y-%m-%d_00:00:00", 
         time.localtime(time.time() - 1800))
   endTime = time.strftime("%Y-%m-%d_24:00:00", 
         time.localtime(time.time() - 1800))
   day = time.strftime("%Y%m%d", 
         time.localtime(time.time() - 1800))
   count = count_new(dest)
   startTime = '2013-09-04_04:00:00'
   endTime = '2013-09-04_07:00:00'
   os.system('java -jar synchronization.jar ' + startTime + ' ' + endTime)
   if synchronization_finished(dest):
      load_data(dest)
   join_tables()



def perform(inc = 300):
   s.enter(inc, 0, perform, (inc,))
   event_func()

def mymain(inc):
   s.enter(0, 0, perform, (inc,))
   s.run()

      
#main
if __name__ == '__main__':
   mymain(30)
#load_data(dest)
   



   






