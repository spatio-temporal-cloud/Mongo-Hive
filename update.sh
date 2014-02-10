tar -zxvf update.tar.gz

rm support_dashboard1.hql.template
rm support_dashboard2.hql.template
rm marketing_dashboard1.hql.template
rm marketing_dashboard2.hql.template
rm marketing_dashboard3.hql.template
rm marketing_dashboard4.hql.template

rm support_dashboard1.hql
rm support_dashboard2.hql
rm marketing_dashboard1.hql
rm marketing_dashboard2.hql
rm marketing_dashboard3.hql
rm marketing_dashboard4.hql

rm deployment.py
rm cronjob.sh

rm lock
rm cronjob.log

touch cronjob.log

cp ./update/support_dashboard1.hql.template ./
cp ./update/support_dashboard2.hql.template ./
cp ./update/marketing_dashboard1.hql.template ./
cp ./update/marketing_dashboard2.hql.template ./
cp ./update/marketing_dashboard3.hql.template ./
cp ./update/marketing_dashboard4.hql.template ./

cp ./update/deployment.py ./
cp ./update/cronjob.sh ./

rm update.tar.gz
rm -rf update



