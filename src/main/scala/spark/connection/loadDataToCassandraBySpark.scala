package spark.connection

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Vector
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import com.datastax.spark.connector.cql.CassandraConnector

class loadDataToCassandraBySpark {

  val conf = ConfigFactory.load()
  val mappingFile = conf.getString("resourcePath") + conf.getString("mappingFile")
  val dataFile = conf.getString("resourcePath") + conf.getString("dataFile")
  val keySpace = conf.getString("keySpace")
  val tableName = conf.getString("table")

  def loadDataToCassndra(): Unit = {

    val sc = SparkContext.context()
    prepareTables(sc)
    // Reading mapping schema file 
    val mappingString = scala.io.Source.fromFile(mappingFile).getLines().mkString
    val mappingArray = mappingString.split(",")

    // Reading data file and split it
    println("start reading file from path " + dataFile)
    val cassandraRDD = sc.cassandraTable(keySpace, tableName)

    val dataRDD = sc.textFile(dataFile)

    val lineRDD = dataRDD.map(line => (line.split(",", mappingArray.length)).map(_.toLong))

    val insertRDD = lineRDD.map { rowMapArray => CassandraRow.fromMap((mappingArray zip rowMapArray) toMap) }

    insertRDD.saveToCassandra(keySpace, tableName)

    println("finished")
  }

  def prepareTables(sc: org.apache.spark.SparkContext): Unit = {
    val keySpaceCQL = scala.io.Source.fromFile(conf.getString("resourcePath") + conf.getString("keySpaceCQL")).getLines().mkString
    val createTableCQL = scala.io.Source.fromFile(conf.getString("resourcePath") + conf.getString("createTableCQL")).getLines().mkString

    var replaceMap = new HashMap[String, String]
    replaceMap.put("{{keySpaceNameVar}}", keySpace)
    replaceMap.put("{{tableNameVar}}", tableName)

    println(Utils.replaceStringFromMap(keySpaceCQL, replaceMap))
    println(Utils.replaceStringFromMap(createTableCQL, replaceMap))
    CassandraConnector(sc).withSessionDo { session =>
      session.execute(Utils.replaceStringFromMap(keySpaceCQL, replaceMap))
      session.execute(Utils.replaceStringFromMap(createTableCQL, replaceMap))
    }
  }
}