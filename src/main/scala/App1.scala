import org.apache.spark.{SparkConf, SparkContext}

object App1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App1").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)


    val file = "hdfs:///cs449/data3_"+args(0)+".csv"
    val data = sc.textFile(file)
    val rows = data.map(line => {
      val c = line.split(",")
      (c(0), c(1).toInt, c(2).toInt)
    })

    val t1 = rows.map(p => p._2 -> p._3/10)
    val t2 = t1.collect.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum)
    t2.foreach(println)
  }
}