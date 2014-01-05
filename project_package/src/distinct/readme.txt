1.compile the .java file
  command:
  javac -classpath hive-exec-0.10.0-cdh4.5.0.jar DistinctLead.java
  
  and we get three .class files :
  DistinctLead$DistinctLeadEvaluator$PartialResult.class
  DistinctLead$DistinctLeadEvaluator.class
  DistinctLead.class
  
2.export the .class files to .jar file
  command
  (JAVA TM)
  jar cvf distinctlead.jar DistinctLead$DistinctLeadEvaluator$PartialResult.class DistinctLead$DistinctLeadEvaluator.class DistinctLead.class hive-exec-0.10.0-cdh4.5.0.jar
  (OpenJDK)
  jar cvf distinctlead.jar DistinctLead\$DistinctLeadEvaluator\$PartialResult.class DistinctLead\$DistinctLeadEvaluator.class DistinctLead.class hive-exec-0.10.0-cdh4.5.0.jar
  
  we get distinctlead.jar 