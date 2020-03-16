import org.apache.spark.{SparkConf, SparkContext}

object App7 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App7").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val file = "hdfs:///cs449/data3_" + args(0) + ".csv"
    val data = sc.textFile(file)
    val rdd1 = data.map(line => {
      val c = line.split(",")
      (c(0), c(2).toInt)
    })
    val rdd2 = sc.textFile("hdfs:///cs449/data4.csv").map(line => {
      val c = line.split(",")
      (c(0), c(1).toInt)
    })
    val filtered = rdd1.join(rdd2).filter { case (k, (v1, v2)) => v1 < v2 }
    val result = filtered.groupBy(_._1).map { case (k, l) => k -> l.map(_._2._1).sum }
    result.collect.foreach(println)
  }
}