#!/bin/bash
workDir=`readlink -f $0`
workDir=`dirname $workDir`
cd $workDir
user=$(whoami)
curDir=${PWD}
#echo "$curDir"


while [ -z $mongodb_server_ip ]
do
   read -p "please specify the mongodb server ip:" mongodb_server_ip
done
echo "$mongodb_server_ip"

while [ -z $mongodb_port ]
do
   read -p "please specify the mongodb port default = 27017:" mongodb_port
done
echo "$mongodb_port"


while [ -z $database_name ]
do
   read -p "please specify the monogdb database name:" database_name
done
echo "$database_name"

read -p "leads collection name default = leads:" leads_collection_name 
leads_collection_name="${leads_collection_name:-leads}"
echo "$leads_collection_name"


read -p "sessions collection name default = sessions: " sessions_collection_name 
sessions_collection_name="${sessions_collection_name:-sessions}"
echo "$sessions_collection_name"


read -p "orders collection name default = orders:" orders_collection_name 
orders_collection_name="${orders_collection_name:-orders}"
echo "$orders_collection_name"


read -p "apicalls collection name default = apicalls:" apicalls_collection_name 
apicalls_collection_name="${apicalls_collection_name:-apicalls}"
echo "$apicalls_collection_name"


read -p "errors collection name default = errors:" errors_collection_name 
errors_collection_name="${errors_collection_name:-errors}"
echo "$errors_collection_name"


read -p "quals collection name default = quals:" quals_collection_name 
quals_collection_name="${quals_collection_name:-quals}"
echo "$quals_collection_name"


read -p "visits collection name default = visits:" visits_collection_name 
visits_collection_name="${visits_collection_name:-visits}"
echo "$visits_collection_name"


read -p "errorCodes collection name default = errorCodes:" errorCodes_collection_name 
errorCodes_collection_name="${errorCodes_collection_name:-errorCodes}"
echo "$errorCodes_collection_name"


read -p "customers collection name default = customers:" customers_collection_name 
customers_collection_name="${customers_collection_name:-customers}"
echo "$customers_collection_name"

#read -p "please specify the directory where you want to store the txt file of data synchronized back from mongodb ? " output_file_dest
output_file_dest=${output_file_dest:-$curDir'/data/'}
if [ -d "$output_file_dest" ]
then
	echo "$output_file_dest" exists
else
	echo "$output_file_dest" does not exists
	mkdir -p "$output_file_dest"
fi
echo "txt files of data synchronized back from mongodb will be in directory ""$output_file_dest"


 
while [ -z $sender ]
do
   read -p "please specify the sender@example.com for reporting errors:" sender
done
echo "$sender"

while [ -z $recipient ]
do
   read -p "please specify the recipient@example.com for reporting errors:" recipient
done
echo "$recipient"

while [ -z $smtp ]
do
   read -p "please specify the smtp.example.com for reporting errors:" smtp
done
echo "$smtp"


mongoimport --file "errorCodes.json" --host "$mongodb_server_ip" --port "$mongodb_port" --db "$database_name" --collection "$errorCodes_collection_name"
if [ $? ]
then
	echo mongoimport errorcodes to "$database_name.""$errorCodes_collection_name" succeded!
else
	echo mongoimport errorcodes to "$database_name.""$errorCodes_collection_name" failed!
        echo installer failed!
	exit 1;
fi
hive -f create_table.hql

if [ $? ]
then
	echo "create tables in hive succeded!"
else
	echo "create tables in hive failed!"
        echo installer failed!
	exit 1;
fi

exec  > "$curDir""/java_sync.properties"
echo "[baseconf]"
echo "mongodb_server_ip=""$mongodb_server_ip"
echo "mongodb_port=""$mongodb_port"
echo "database_name=""$database_name"
echo "leads_collection_name=""$leads_collection_name"
echo "sessions_collection_name=""$sessions_collection_name"
echo "orders_collection_name=""$orders_collection_name"
echo "apicalls_collection_name=""$apicalls_collection_name"
echo "errors_collection_name=""$errors_collection_name"
echo "quals_collection_name=""$quals_collection_name"
echo "visits_collection_name=""$visits_collection_name"
echo "errorCodes_collection_name=""$errorCodes_collection_name"
echo "customers_collection_name=""$customers_collection_name"
echo "output_file_dest=""$output_file_dest"

exec > "$curDir""/alarm.properties"
echo "sender=""$sender"
echo "recipient=""$recipient"
echo "smtp=""$smtp"

exec > "$curDir""/cron.txt"
cronjob="$curDir""/cronjob.sh"
echo "*/30 * * * * ""$cronjob"" >> ""$curDir""/cronjob.log" "2>&1" 

exec > /dev/tty
sudo crontab -u "$user" cron.txt

if [ $? ]
then
	echo "installing crontab for cronjob.sh succeded!"
else
	echo "installing crontab for cronjob.sh failed!"
	echo "installer failed!"
        exit 1
fi
echo "installing completes"













