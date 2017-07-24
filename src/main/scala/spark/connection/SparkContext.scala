package spark.connection

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

 object SparkContext {

  def session(): org.apache.spark.sql.SparkSession = {
    val configFile = ConfigFactory.load()
    val cassandraHost = configFile.getString("cassandraHost")
    val sparkMemory = configFile.getString("sparkMemory")

    println("Entering context")
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark-to-cassandra-load")
      .config("spark.executor.memory", sparkMemory)
      .config("spark.cassandra.connection.host", cassandraHost)
      .config("start_native_transport", "true")
      .getOrCreate()
    
    return sparkSession

  }
}