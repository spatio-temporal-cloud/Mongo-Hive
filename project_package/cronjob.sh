#!/bin/bash

workDir=`readlink -f $0`
workDir=`dirname $workDir`
cd $workDir

date1='date  +%Y-%m-%d-%H:%M:%S%t'


source ${workDir}/alarm.properties
source /etc/environment
source /etc/profile
source ~/.bashrc

if [ -e lock ]
then
        true
 	#echo "lock exists"
else
	#echo "lock does not exists"
	echo "0" > lock
fi

lock=`cat lock`
#is_running=`ps aux | grep "$pid" | grep cronjob.sh | grep -v "grep" | grep -v "defunct"`
if [ "$lock" == "1" ]
then
  echo "last job is running, exit"
  echo 
  exit 0
fi

echo "1" > lock
#echo $$
#echo "run deployment"
#echo `java -version`


time=`date +%s`
time=$((time - 1800))
day1=`date -d @$time +%Y%m%d`
day2=`date -d @$time +%Y-%m-%d`

echo
echo `$date1`"starting deployment.py for day "$day1
python deployment.py $day1 $day2
ret=$?
echo "0" > lock
#echo "$ret"
#exit 0

if [ "$ret" != 0 ] 
then
   echo `$date1`"deployment.py failed for day "$day1
   echo "Deployment.py failed for certain reasons, please check cronjob.log for more information!" | sendmail -F 'hadoop@bridgevine.com' -t 'mingyuzeng@sinonovatechnology.com.cn' 
else
   echo `$date1`"deployment.py finised for day "$day1   
fi

echo 


