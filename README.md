#Mongodb Data Synchronization

##Before Install

* The following softwares should be installed before running our installer:
java, python, hadoop, hive with mysql as the metastore, mongodb. For your information we use java7, python 2.7.6, hadoop 4U5, hive 4U5 mongodb2.4, mysql 5.6.15 on our server.

* export JAVA_HOME, HADOOP_HOME, HIVE_HOME in ~/.bashrc, add the directories JAVA_HOME/bin, HADOOP_HOME/bin, HIVE_HOME/bin to  the environment variable PATH in ~/.bashrc. This step is to make sure that the binaries of these softwares are in the PATH.

* The installer will create a database "maize" on hive, so please make sure hive does not have a database with the same name, it will also create all the necessary tables in the "maize" database.

* The installer will replace the crontab of current user, so please make sure you have backuped the crontab of current user, so you can add them after running the installer. It will install a crontab for "cronjob.sh" and redirect the standard out and standard err out to log "cronjob.log" under the directory of installer

* The installer will mongoimport the json file "errorCodes.json" to a collection in the mongodb database storing your data, so please specify a name for the errorCodes that does not exist a collection with this name in the mongodb database.

* The installer will create three files under the directory of installer: "java_sync.properties" which contains the necessary arguments for the java synchronization program, "alarm.propertis" which contains the sender, recipient and smtp for the command "sendmail" to send alerting emails when the program goes wrong, "cron.txt" which is the crontab commands for the program.

* The intaller will create a directory "data" under the directory of installer, this "data" directory will contain the txt files of data synchronized back from mongodb before loading to hive with the file extension ".new" and the time expansion the data as the name. The file extenstion will be ".old" after loading to hive. 

(8) The file "cronjob.log"  under the directroy of installer contains the log of the cronjob, if anything goes wrong, we can locate problems using this log, both standard out and standard error out are redirected to "cronjob.log".

(9) After the installer completes successfully, the cronjob.sh will be in crontab and it will run automatically.

(10) The "cronjob.log" will contain executing information and standard err out of each phrase of the synchronization: java synchronization, loading data to hive, joinning tables in hive, if any one of these phrase failed, there will be information indicating which phrase has failed, so we can locate problems.

(11) If you ever want to stop the cronjob.sh, using the following commands:
crontab -e, then comment the commands with cronjob.sh with "\#"  

##Install

(1) tar the package with the commands: 

      tar -zxvf project_package.tar.gz
      
(2) change directory  with the  commands: 

      cd project_package
      
(3) add execute permissions to the installer.sh, cronjob.sh, deployment.py with 
the commands:

      sudo chmod a+x installer.sh
      sudo chmod a+x cronjob.sh
      sudo chmod a+x deployment.py
      
(4) run installer.sh with the commands: 

      ./installer.sh
      
(5) specify the IP of mongodb server
(6) specify the port of mongodb
(7) specify the name of mongodb database storing the data
(8) specify the leads collection name, if it's leads, just press enter
(9) specify the sessions collection name, if it's sessions, just press enter
(10) specify the orders collection name, if it's orders, just press enter
(11) specify the apicalls collection name, if it's apicalls, just press enter
(12) specify the errors collection name, if it's errors, just press enter
(13) specify the quals collection name, if it's quals, just press enter
(14) specify the visits collection name, if it's visits, just press enter
(15) specify the erroCodes collection name, if it's errorCodes, just press enter
(16) specify the customers collection name, if it's customers, just press enter
(17) specify the sender@example.com for reporting errors
(18) specify the recipient@example.com for reporting errors
(19) specify the smtp.example.com for reporting errors
(20) enter passwd of current user for installing crontab for the program








