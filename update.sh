#!/bin/bash

rm -rf update/*

rm create_table.hql
rm deployment.py
#rm cronjob.sh
#rm installer.sh
#rm synchronization.jar
rm old_data.sh

rm support_dashboard1.hql.template
rm support_dashboard2.hql.template
rm marketing_dashboard1.hql.template
rm marketing_dashboard2.hql.template
rm marketing_dashboard3.hql.template
rm marketing_dashboard4.hql.template


tar -zxvf update.tar.gz


cp ./update/support_dashboard1.hql.template ./
cp ./update/support_dashboard2.hql.template ./
cp ./update/marketing_dashboard1.hql.template ./
cp ./update/marketing_dashboard2.hql.template ./
cp ./update/marketing_dashboard3.hql.template ./
cp ./update/marketing_dashboard4.hql.template ./

cp ./update/deployment.py ./
#cp ./update/cronjob.sh ./
#cp ./update/synchronization.jar ./
cp ./update/old_data.sh ./
cp ./update/create_table.hql ./
#cp ./update/installer.sh ./

chmod a+x deployment.py 
#chmod a+x cronjob.sh
#chmod a+x synchronization.jar
chmod a+x old_data.sh
#chmod a+x installer.sh

hive -f create_table.hql >> cronjob.log
nohup ./old_data.sh >> cronjob.log 2>&1 &
#./old_data.sh >> old_data.log 2>&1 &



