import org.apache.spark.{SparkConf, SparkContext}

object App2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App2").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)


    val file = "hdfs:///cs449/data3_"+args(0)+".csv"
    val data = sc.textFile(file,200)
    val rows = data.map(line => {
      val c = line.split(",")
      (c(0), c(1).toInt, c(2).toInt)
    })


    // App 2 is very similar to App 1 except that when calling textFile method, we set a minPartitions too 200.
    // App 2 is doing exactly the same operation as in App 1. For further information, please refer our documentation in App 1.

    // When use textFile method, data will be divided into partitions. The default size of a block in HDFS is 128 MB.
    // The large dataset has size 1.4 GB which is divided into 12 partitions. However, in App 2, we are having 200 partitions.
    // Having more partitions might reduce runtime. However, this does not reduce data being transferred over the network since
    // the codes were using groupBy.
    // Thus, besides OOM error, the program also showed TaskResultLost error. This is usually due to the fact that we are bringing
    // a large amount of data into driver, which does not fit the driver node.

    // Our solution is firstly to replace groupBy and map by reduceByKey in order to reduce the amount of data shuffling over the network.
    // Then, we need also to call "collect" on smaller data. But since reduceByKey reduces to minimal amount of results, we can
    // "collect" right after. The summation part in the last "map" function is already done by reduceByKey.

    // Note that the final instance t2 has originally type Map[Int, Int], we add "toMap" to keep the same type.

    val t1 = rows.map(p => p._2 -> p._3/10)
    // original codes
    //val t2 = t1.collect.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum)

    // optimized codes
    val t2 = t1.reduceByKey(_+_).collect.toMap
    t2.foreach(println)
  }
}
