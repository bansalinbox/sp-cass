# sp-cass

If there are showing errors in the project 
Right click on project -> configure -> add scala nature

Run the cassandra cluster first by on command prompt: cqlsh

If want to run this project using the fat jar run with the following command : 

spark-submit --conf spark.cassandra.connection.host=127.0.0.1 --class spark.connection.loadDataToCassandra target/sp-cass-0.0.1-SNAPSHOT-jar-with-dependencies.jar

otherwise use :
spark-submit --conf spark.cassandra.connection.host=127.0.0.1 --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 --class spark.connection.loadDataToCassandra target/sp-cass-0.0.1-SNAPSHOT-jar-with-dependencies.jar

Please mail to bansalinbox@gmail.com if need any help in setting this project.