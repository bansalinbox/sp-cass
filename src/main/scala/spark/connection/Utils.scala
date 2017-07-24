package spark.connection

import collection.mutable.HashMap
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf

object Utils {

  def replaceStringFromMap(str: String, map: HashMap[String, String]): String = {
    var stringVal = str;
    map.foreach({
      x => stringVal = replaceString(stringVal, x._1, x._2)
    });
    return stringVal;
  }

  def replaceString(str: String, to: String, value: String): String = {
    str.replace(to, value)
  }

  def executeCassndraCQl(sc: org.apache.spark.SparkContext, cqlString: String): Unit = {
    println("Going to execute sql -->" + cqlString)
    CassandraConnector(sc).withSessionDo { session =>
      session.execute(cqlString)
    }
  }

}

