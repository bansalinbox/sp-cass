package spark.connection

import scala.io.Source
import collection.mutable.HashMap
object arrayDemo {
  
 def main(args : Array[String]){
    
    val map = new HashMap[String, String]
    map.put("{{needToReplace}}", "test3")
    map.put("{{keyspace1}}", "key")
    
    val str = "keyspace name is {{keyspace1}} and value is {{needToReplace}}"
    
    Utils.replaceStringFromMap(str, map);
    
  }

}