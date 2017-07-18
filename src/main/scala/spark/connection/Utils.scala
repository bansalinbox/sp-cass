package spark.connection

import collection.mutable.HashMap

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

}

