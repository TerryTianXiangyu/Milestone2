import org.apache.spark.{SparkConf, SparkContext}

object App4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App4").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1").
      set("spark.driver.memory", "10g")

    val sc = SparkContext.getOrCreate(conf)

    val file = "hdfs:///cs449/data3_"+args(0)+".csv"
    val data = sc.textFile(file,200)
    val rows = data.map(line => {
      val c = line.split(",")
      (c(0), c(1).toInt, c(2).toInt)
    })

    // App 4 is very similar to App 2 except that it has an additional configuration. The driver memory is set to 10G, which
    // is 1G by default (in App 1 and App 2). App 4 is doing exactly the same operation as in App 1 and App 2. For further
    // information, please refer our documentation in App 1.

    // In App 2, we did not have enough memory in the driver to get all computed results. Here we have more spaces in the driver.
    // However, this time, we have another error due to the raise of driver memory. The error is "Job aborted due to stage failure:
    // Total size of serialized results of 102 tasks (1026.5 MB) is bigger than spark.driver.maxResultSize (1024.0 MB) "

    // When we increase the driver memory, we better adapt the corresponding driver.maxResultSize. But in our project we cannot
    // change the configuration, we need to change codes only. From the root of problem, collecting all distributed results into
    // a single driver node causes the problem.
    // Solution can be to reduce the amount of the results (so reduce result size) then send them to the driver. This is done by
    // replacing groupBy and map by reduceByKey. We combine firstly some elements on each partition, reducing the elements being
    // transferred over the network, then sum corresponding values together.

    // Note that the final instance t2 has originally type Map[Int, Int], we add "toMap" to keep the same type.

    val t1 = rows.map(p => p._2 -> p._3/10).reduceByKey(_+_)
    // original codes
    //val t2 = t1.collect.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum)

    // optimized codes
    val t2 = t1.reduceByKey(_+_).collect.toMap
    t2.foreach(println)
  }
}
