import sys, os, stat
import sched
import types, time
import re, ConfigParser


cf = ConfigParser.ConfigParser()
cf.read("java_sync.properties")
dest = cf.get("baseconf", "output_file_dest")
#dest = '/home/zengmingyu/data'
curDir = os.getcwd()
#hive_sql='select count(*) from ssp_factbids'
load_prefix = "load data local inpath " 
load_midfix =  " overwrite into table "
load_suffix = " partition (day=\""
day=20140101




def get_table(filePath):
   fileName =  os.path.splitext(os.path.basename(filePath))[0]
   return fileName.split('_')[2]


def load_data(path):
   loadhql = open("loaddata.hql", "w")
   loadhql.write("use maize;\n")
   for item in os.listdir(path):
      match =  re.match(r'\w+\_\w+\_\w+\.new', item) 
      if match:
         item= os.path.join(path, item)
         if os.path.isfile(item) and os.path.splitext(item)[1] == '.new':
            table = get_table(item) 
            #if table == 'orders' or table == 'customers' or table == 'visits':
               #continue
#print table
#            print os.path.basename(item)
            hiveql = load_prefix + '\'' + item + '\'' + load_midfix + table + load_suffix + day + "\");\n"
            loadhql.write(hiveql)
#            print hiveql
   loadhql.close()
   ret = os.system("hive -S -f loaddata.hql")

   loadhql = os.path.join(os.getcwd(), "loaddata.hql")
   os.remove(loadhql)

   for item in os.listdir(path):
      match =  re.match(r'\w+\_\w+\_\w+\.new', item) 
      if match:
         item = os.path.join(path, item)
         old = os.path.splitext(item)[0] + '.old'
         os.rename(item, old)
   if ret != 0:
      print "load data to hive failed"
      return 1

   return 0   
  
def join_tables():
   #print "joinning tables"
   #print day
   marketing_dashboard_joinhql1 = "sed -e \'s/\[DAY\]/" + day + "/g\' marketing_dashboard1.hql.template > marketing_dashboard1.hql" 
   marketing_dashboard_joinhql2 = "sed -e \'s/\[DAY\]/" + day + "/g\' marketing_dashboard2.hql.template > marketing_dashboard2.hql" 
   marketing_dashboard_joinhql3 = "sed -e \'s/\[DAY\]/" + day + "/g\' marketing_dashboard3.hql.template > marketing_dashboard3.hql" 
   marketing_dashboard_joinhql4 = "sed -e \'s/\[DAY\]/" + day + "/g\' marketing_dashboard4.hql.template > marketing_dashboard4.hql" 
   support_dashboard_joinhql1 = "sed -e \'s/\[DAY\]/" + day + "/g\' support_dashboard1.hql.template > support_dashboard1.hql" 
   support_dashboard_joinhql2 = "sed -e \'s/\[DAY\]/" + day + "/g\' support_dashboard2.hql.template > support_dashboard2.hql" 

   ret = os.system(support_dashboard_joinhql1)
   if ret != 0:
      return 1

   ret = os.system(support_dashboard_joinhql2)
   if ret != 0:
      return 1

   ret = os.system(marketing_dashboard_joinhql1)
   if ret != 0:
      return 1

   ret = os.system(marketing_dashboard_joinhql2)
   if ret != 0:
      return 1

   ret = os.system(marketing_dashboard_joinhql3)
   if ret != 0:
      return 1

   ret = os.system(marketing_dashboard_joinhql4)
   if ret != 0:
      return 1

   ret = os.system("hive -f support_dashboard1.hql ")
   if ret != 0:
      return 1

   ret = os.system("hive -f support_dashboard2.hql ")
   if ret != 0:
      return 1

   ret = os.system("hive -f marketing_dashboard1.hql ")
   if ret != 0:
      return 1

   ret = os.system("hive -f marketing_dashboard2.hql ")
   if ret != 0:
      return 1

   ret = os.system("hive -f marketing_dashboard3.hql ")
   if ret != 0:
      return 1

   ret = os.system("hive -f marketing_dashboard4.hql ")
   if ret != 0:
      return 1
  
   return 0
  
   #print marketing_dashboard_joinhql
   #print support_dashboard_joinhql



def mymain(day1, day2):
   global day;
   day = day1
   startTime = day2 + '_00:00:00'
   endTime = day2 + '_24:00:00'
   propPath = curDir + '/java_sync.properties'
   
   #day="20140210"
   #startTime = '2014-02-10_00:00:00'
   #endTime = '2014-02-10_24:00:00'
   
   #print day
   #print startTime
   #print endTime
   #return 0
   
   print time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time())) + ' ',
   print "java -jar synchronization is running...." 
   ret = os.system('java -jar synchronization.jar ' + startTime + ' ' + endTime 
+ ' ' + propPath)
   print time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time())) + ' ',   
   if ret != 0:
      print "java -jar synchronization failed!"
      return 1
   print "java -jar synchronization finished!"	
   

  
   #return 0
   print time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time())) + ' ',
   print "loading data to hive....."
   ret = load_data(dest)      
   print time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time())) + ' ',
   if ret != 0:
      print "loading data to hive failed!"
      return 1
   print "loading data to hive finished!"

   print time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time())) + ' ',
   print "joinning tables in hive..."
   print  "hive join day = " + day 
   ret = join_tables()
   #ret = 0
   print time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time())) + ' ',
   if ret != 0:
      print "joinning tables in hive failed!"
      return 1
   print "joinning tables in hive finished!"
   return 0


      
#main
if __name__ == '__main__':
   #join_tables()
   #print "starting deployment.py...."
   day1 = sys.argv[1]
   day2 = sys.argv[2]
   if mymain(day1, day2) != 0:
      #print "deployment.py failed!"
      sys.exit(1)
   #print "deployment.py finished!"
   #load_data(dest)
  

   






