import org.apache.spark.{SparkConf, SparkContext}


object App6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App6").
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
    val t1 = rows.groupBy(_._1)
    val t2 = t1.map(kl => kl._1 -> kl._2.groupBy(_._2).map(bc => bc._1 -> bc._2.map(_._3).sum))
    val t3 = t2.flatMap(kll => kll._2.map(kv => (kll._1, kv._1) -> (kv._2)))
    t3.collect.foreach(println)

  }
}