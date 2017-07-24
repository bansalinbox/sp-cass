package spark.connection

import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Vector
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector._

import org.apache.spark.sql.SaveMode

class loadDataToCassandraBySpark {

  val conf = ConfigFactory.load()
  val keySpace = conf.getString("keySpace")
  val resourcePath = conf.getString("resourcePath")

  def loadDataToCassndra(): Unit = {

    val session = SparkContext.session()
    val sc = session.sparkContext

    // Create and load nesessary tables
    prepareTables(sc)

    // Loading the sample state aggregate table
    loadAggregate_Sample_State_Table(session)
    loadAggregate_Sample_Race_Table(session)
    loadAggregate_Sample_Age_Table(session)

  }

  def prepareTables(sc: org.apache.spark.SparkContext): Unit = {
    println("Start creating and load the base tables")
    loadKeySpaceForAll(sc)
    loadSampleFileToCassandra(sc)
    // Sample -- state 
    loadStateFileToCassandra(sc)
    prepareSampleStateTableInCassandra(sc)

    // Sample -- race
    loadRaceTypeFileToCassandra(sc)
    prepareSampleRaceTypeTableInCassandra(sc)

    // Sample -- age
    loadAgeTypeFileToCassandra(sc)
    prepareSampleAgeTypeTableInCassandra(sc)

    println("END creating and load the base tables")
  }
  
  def loadAggregate_Sample_Age_Table(session: org.apache.spark.sql.SparkSession): Unit = {

    val sample_ageTypeTableName = conf.getString("sampleAgeTypeTableName")
    val cassandra_host = conf.getString("cassandraHost")
    val sampleTable = conf.getString("sampleTableName")

    val sampleDF = session.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sampleTable, "keyspace" -> keySpace))
      .load() // This Dataset will use a spark.cassandra.input.size of 128
    sampleDF.createOrReplaceTempView("sample")

    val ageTypeTable = conf.getString("ageTypeTableName")
    val ageTypeDF = session.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> ageTypeTable, "keyspace" -> keySpace))
      .load()
    ageTypeDF.createOrReplaceTempView("age_type")

    val cassandraRDD = session.sql("select age, count, bround((count/(select count(*) from sample))*100,2) as percent, age_type from (SELECT s.age as age , st.age_type as age_type, count(*) as count FROM age_type st right join sample s on st.age = s.age group by s.age, st.age_type )")
    cassandraRDD.show()
    cassandraRDD.write.option("confirm.truncate", true)
      .mode(SaveMode.Overwrite)
      .cassandraFormat(sample_ageTypeTableName, keySpace, cassandra_host)
      .save()
  }
  
  def loadAggregate_Sample_Race_Table(session: org.apache.spark.sql.SparkSession): Unit = {

    val sample_raceTypeTableName = conf.getString("sampleRaceTypeTableName")
    val cassandra_host = conf.getString("cassandraHost")
    val sampleTable = conf.getString("sampleTableName")

    val sampleDF = session.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sampleTable, "keyspace" -> keySpace))
      .load() // This Dataset will use a spark.cassandra.input.size of 128
    sampleDF.createOrReplaceTempView("sample")

    val raceTypeTable = conf.getString("raceTypeTableName")
    val raceTypeDF = session.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> raceTypeTable, "keyspace" -> keySpace))
      .load()
    raceTypeDF.createOrReplaceTempView("race_type")

    val cassandraRDD = session.sql("select race, count, bround((count/(select count(*) from sample))*100,2) as percent, race_type from (SELECT s.race as race , st.race_type as race_type, count(*) as count FROM race_type st right join sample s on st.race = s.race group by s.race, st.race_type )")
    cassandraRDD.show()
    cassandraRDD.write.option("confirm.truncate", true)
      .mode(SaveMode.Overwrite)
      .cassandraFormat(sample_raceTypeTableName, keySpace, cassandra_host)
      .save()
  }

  def loadAggregate_Sample_State_Table(session: org.apache.spark.sql.SparkSession): Unit = {

    val sample_state = conf.getString("sampleStateTableName")
    val cassandra_host = conf.getString("cassandraHost")
    val sampleTable = conf.getString("sampleTableName")
    val sampleDF = session.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sampleTable, "keyspace" -> keySpace))
      .load() // This Dataset will use a spark.cassandra.input.size of 128
    sampleDF.createOrReplaceTempView("sample")

    val stateTable = conf.getString("stateTableName")
    val stateDF = session.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> stateTable, "keyspace" -> keySpace))
      .load()
    stateDF.createOrReplaceTempView("state")

    val cassandraRDD = session.sql("select stfips, count, bround((count/(select count(*) from sample))*100,2) as percent, state from (SELECT s.stfips as stfips , st.state as state, count(*) as count FROM state st right join sample s on st.stfips = s.stfips group by s.stfips, st.state )")
    cassandraRDD.show()
    cassandraRDD.write.option("confirm.truncate", true)
      .mode(SaveMode.Overwrite)
      .cassandraFormat(sample_state, keySpace, cassandra_host)
      .save()
  }

  def prepareSampleAgeTypeTableInCassandra(sc: org.apache.spark.SparkContext): Unit = {
    val ageTypePath = fullPath(conf.getString("ageTypePath"))
    val tableCQl = readFile(ageTypePath + conf.getString("createSampleAgeTableCQL"))
    val tableName = conf.getString("sampleAgeTypeTableName")

    prepareSampleTables(sc, tableName, tableCQl)
  }
  def prepareSampleRaceTypeTableInCassandra(sc: org.apache.spark.SparkContext): Unit = {
    val raceTypePath = fullPath(conf.getString("raceTypePath"))
    val tableCQl = readFile(raceTypePath + conf.getString("createSampleRaceTypeTableCQL"))
    val tableName = conf.getString("sampleRaceTypeTableName")

    prepareSampleTables(sc, tableName, tableCQl)
  }
  def prepareSampleStateTableInCassandra(sc: org.apache.spark.SparkContext): Unit = {
    val statePath = fullPath(conf.getString("statePath"))
    val tableCQl = readFile(statePath + conf.getString("createSampleStateTableCQL"))
    val tableName = conf.getString("sampleStateTableName")

    prepareSampleTables(sc, tableName, tableCQl)
  }
  def fullPath(str: String): String = {
    resourcePath.concat(str)
  }

  def loadSampleFileToCassandra(sc: org.apache.spark.SparkContext): Unit = {
    // TODO : Make it more flexible
    val samplePath = fullPath(conf.getString("samplePath"))
    val sampleSchemaPath = samplePath + conf.getString("sampleSchema")
    val sampleDataFile = samplePath + conf.getString("sampleDataFile")
    val sampleTableCQl = readFile(samplePath + conf.getString("createSampleTableCQL"))

    val tableName = conf.getString("sampleTableName")
    prepareSampleTables(sc, tableName, sampleTableCQl)

    val sampleSchema = readFile(sampleSchemaPath).split(",")

    // Reading data file and split it
    println("start reading file from path " + sampleDataFile)

    val dataRDD = sc.textFile(sampleDataFile)
    val lineRDD = dataRDD.map(line => {
      (line.split(",", sampleSchema.length)).map(_.toLong)
    })
    val insertSampleRDD = lineRDD.map { rowMapArray => CassandraRow.fromMap((sampleSchema zip rowMapArray) toMap) }
    insertSampleRDD.saveToCassandra(keySpace, tableName)
  }

  def loadStateFileToCassandra(sc: org.apache.spark.SparkContext): Unit = {
    // TODO : Make it more flexible
    val statePath = fullPath(conf.getString("statePath"))
    val schemaPath = statePath + conf.getString("stateSchema")
    val dataFile = statePath + conf.getString("stateDataFile")
    val tableCQl = readFile(statePath + conf.getString("createStateTableCQL"))
    val tableName = conf.getString("stateTableName")

    prepareSampleTables(sc, tableName, tableCQl)

    // Reading mapping schema file 
    val schema = readFile(schemaPath).split(",")

    // Reading data file and split it
    println("start reading file from path " + dataFile)
    val dataRDD = sc.textFile(dataFile)
    val lineRDD = dataRDD.map(line => {
      line.split(",", schema.length)
    })
    val insertSampleRDD = lineRDD.map { rowMapArray => CassandraRow.fromMap((schema zip rowMapArray) toMap) }
    insertSampleRDD.saveToCassandra(keySpace, tableName)
  }

  def loadAgeTypeFileToCassandra(sc: org.apache.spark.SparkContext): Unit = {
    val ageTypePath = fullPath(conf.getString("ageTypePath"))
    val schemaPath = ageTypePath + conf.getString("ageTypeSchema")
    val dataFile = ageTypePath + conf.getString("ageTypeData")
    val tableCQl = readFile(ageTypePath + conf.getString("createAgeTypeCQL"))
    val tableName = conf.getString("ageTypeTableName")

    prepareSampleTables(sc, tableName, tableCQl)

    // Reading mapping schema file 
    val schema = readFile(schemaPath).split(",")

    // Reading data file and split it
    println("start reading file from path " + dataFile)

    val dataRDD = sc.textFile(dataFile)
    val lineRDD = dataRDD.map(line => {
      line.split(",", schema.length)
    })
    val insertSampleRDD = lineRDD.map { rowMapArray => CassandraRow.fromMap((schema zip rowMapArray) toMap) }
    insertSampleRDD.saveToCassandra(keySpace, tableName)
  }

  def loadRaceTypeFileToCassandra(sc: org.apache.spark.SparkContext): Unit = {
    // TODO : Make it more flexible
    val raceTypePath = fullPath(conf.getString("raceTypePath"))
    val schemaPath = raceTypePath + conf.getString("raceTypeSchema")
    val dataFile = raceTypePath + conf.getString("raceTypeData")
    val tableCQl = readFile(raceTypePath + conf.getString("createRaceTypeCQL"))
    val tableName = conf.getString("raceTypeTableName")

    prepareSampleTables(sc, tableName, tableCQl)

    // Reading mapping schema file 
    val schema = readFile(schemaPath).split(",")

    // Reading data file and split it
    println("start reading file from path " + dataFile)

    val dataRDD = sc.textFile(dataFile)
    val lineRDD = dataRDD.map(line => {
      line.split(",", schema.length)
    })
    val insertSampleRDD = lineRDD.map { rowMapArray => CassandraRow.fromMap((schema zip rowMapArray) toMap) }
    insertSampleRDD.saveToCassandra(keySpace, tableName)
  }

  def prepareSampleTables(sc: org.apache.spark.SparkContext, tableName: String, cql: String): Unit = {
    var replaceMap = new HashMap[String, String]
    replaceMap.put("{{keySpaceNameVar}}", keySpace)
    replaceMap.put("{{tableNameVar}}", tableName)
    val cassandraTableCQl = Utils.replaceStringFromMap(cql, replaceMap)
    Utils.executeCassndraCQl(sc, cassandraTableCQl)
  }

  def readFile(schemaPath: String): String = {
    scala.io.Source.fromFile(schemaPath).getLines().mkString
  }

  def loadKeySpaceForAll(sc: org.apache.spark.SparkContext): Unit = {
    val keySpaceCQL = scala.io.Source.fromFile(conf.getString("resourcePath") + conf.getString("keySpaceCQL")).getLines().mkString
    var replaceMap = new HashMap[String, String]
    replaceMap.put("{{keySpaceNameVar}}", keySpace)

    val keySpaceCQl = Utils.replaceStringFromMap(keySpaceCQL, replaceMap)
    Utils.executeCassndraCQl(sc, keySpaceCQl)
  }
}