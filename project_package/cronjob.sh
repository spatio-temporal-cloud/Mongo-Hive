#!/bin/bash

workDir=`readlink -f $0`
workDir=`dirname $workDir`
cd $workDir


source ${workDir}/alarm.properties
source /etc/environment
source /etc/profile
source ~/.bashrc
echo
echo `date`
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

echo "starting deployment.py...."
python deployment.py
ret=$?
echo "0" > lock
#echo "$ret"

if [ "$ret" != 0 ] 
then
   echo "deployment.py failed!"
   sendMail -f $sender -t $recipient -m "process data failed" -s $smtp
else
   echo "deployment.py finised!"   
fi

echo 


