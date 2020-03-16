import org.apache.spark.{SparkConf, SparkContext}

object App5 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App5").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data5_" + args(0) +".csv")
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt
      (k, v)
    })
    val extendedRows = rows.mapPartitions(it  => {
      val l = it.toList
      val l2 = l.flatMap(x => (2 to 50).map(i => (x._1, x._2 % i)))
      l2.iterator
    })
    val sum = extendedRows.groupBy(_._1).map { case (k, l) => k -> l.map(_._2).sum}
    sum.collect().foreach(println)
  }
}