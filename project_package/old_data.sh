#!/bin/bash

workDir=`readlink -f $0`
workDir=`dirname $workDir`
cd $workDir

date1='date  +%Y-%m-%d-%H:%M:%S%t'


start=20140101
end=20140307




date -d $start +%Y%m%d > /dev/null
if [ $? -ne 0 ]; then
  echo "start date "$start " format is wrong"
  exit 1
fi   


date -d $end +%Y%m%d > /dev/null
if [ $? -ne 0 ]; then
   echo "end date "$end " format is wrong"
   exit 1
fi


startTime=`date -d $start +%s`
#startTime=$((startTime+86400))
endTime=$end
endTime=`date -d $endTime +%s`

while [ $startTime -lt $endTime ]
do
   day1=`date -d @$startTime +%Y%m%d` 
   day2=`date -d @$startTime +%Y-%m-%d`
   #echo $day1 " " $day2
   echo
   echo `$date1`"deployment.py start for "$day1
   python deployment.py $day1 $day2
   ret=$?
   if [ $ret == 0 ]; then
       echo `$date1`"deployment.py succeed for "$day1
    else
       echo `$date1`"deployment.py fail for "$day1
   fi
   echo 
  
   startTime=$((startTime+86400))

done

