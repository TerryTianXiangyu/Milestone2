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
    // Having more partitions might reduce runtime. However, it is horrible to shuffle elements from at least 200 partitions.
    // A lot of data are transferred over the network while they are not necessary to.
    // Using groubBy, we have much more partitions than in App 1, so much more element to shuffle and gather to the driver.
    // Thus, the program launched TaskResultLost error. This is usually due to the fact that we are bringing a large amount
    // of data into driver, which does not fit the driver node.

    // Our solution is firstly to move "collect" at the end of the rdd operation, so that more applications can be perform in paraller
    // and reduce the amount of data collected to the driver. Secondly, as aforementioned, we want to reduce the shuffling over the
    // network. Thus before transfer data, we reduce the amount of elements in each partitions using reduceByKey, which saves a lot
    // of time.

    val t1 = rows.map(p => p._2 -> p._3/10).reduceByKey(_+_)
    val t2 = t1.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum).collect
    t2.foreach(println)
  }
}
