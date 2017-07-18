package test

import com.typesafe.config.ConfigFactory

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
      val mappingFile = "src/main/resources/sampleSchema.txt"
    val dataFile = "src/main/resources/sample.txt"
    val mappingString = scala.io.Source.fromFile(dataFile).getLines().toVector
   val v2 = mappingString.map ( line => line.split(",").map (_.toLong) )
   //println(v2.foreach {println })

  val conf = ConfigFactory.load()
    
    println(conf.getString("test"))
    
  

    
  }

}
