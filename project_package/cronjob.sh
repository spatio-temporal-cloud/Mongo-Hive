#!/bin/bash

workDir=`readlink -f $0`
workDir=`dirname $workDir`
cd $workDir


source ${workDir}/alarm.properties
#source /etc/environment
#source /etc/profile
source ~/.bashrc
echo `date`
if [ -e lock ]
then
        true
 	#echo "lock exists"
else
	#echo "lock does not exists"
	echo "212390123" > lock
fi

pid=`cat lock`
is_running=`ps aux | grep "$pid" | grep cronjob.sh | grep -v "grep" | grep -v "defunct"`
if [ "x$is_running" != "x" ]
then
  echo "last job is running, exit"
  echo ""
  exit 0
fi

echo $$ > lock
#echo $$
#echo "run deployment"
#echo `java -version`
echo "starting deployment.py...."
python deployment.py
#echo $?

if [ $? != 0 ] 
then
   echo "deployment.py failed!"
   sendMail -f $sender -t $recipient -m "process data failed" -s $smtp
else
   echo "deployment.py finised!"   
fi

echo ""


