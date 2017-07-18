package spark.connection

object loadDataToCassandra {
  def main(args: Array[String]) {
    println("Starting process to load data into cassandra")
    val loadData = new loadDataToCassandraBySpark()
    loadData.loadDataToCassndra()
  }
}