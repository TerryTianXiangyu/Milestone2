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

/*
    App 2 is very similar to App 1 except that when calling textFile method, we set a minPartitions too 200.
    App 2 is doing exactly the same operation as in App 1. For further information, please refer our documentation in App 1.

    When use textFile method, data will be divided into partitions. The default size of a block in HDFS is 128 MB.
    The large dataset has size around 1.4 GB which is divided into around 12 partitions. However, in App 2, we are having 200 partitions.
    Having more partitions might reduce runtime with more executors. However, this does not reduce data being transferred over the network since
    the codes were using groupBy.
    Exception messages for App 2 are: "java.lang.OutOfMemoryError: GC overhead limit exceeded", "java.lang.OutOfMemoryError:
    Java heap space" and "BlockManagerMasterEndpoint: No more replicas available for taskresult_xx"

    The driver is running out of memory due to "collect at App2.scala:23". We collect large amount
    of data from workers and try to fit it into a single driver node. Since we cannot modify configuration(i.e. increase executor memory), we can try to reduce the
    executor workload by reducing the amount of data being transferred over the network and rewriting efficient codes.

    Our solution is firstly to replace groupBy and map by reduceByKey in order to reduce the amount of data shuffling over
    the network. Since reduceByKey reduces to minimal amount of results, we can "collect" right after. The summation part in
    the last "map" function is already done by reduceByKey.

    Note that the final instance t2 has originally type Map[Int, Int], we add "toMap" to keep the same type.
*/
    val t1 = rows.map(p => p._2 -> p._3/10)
    // original codes
    //val t2 = t1.collect.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum)

    // optimized codes
    // replace groupBy and sum by reduceByKey, map tp toMap
    val t2 = t1.reduceByKey(_+_).collect.toMap
    t2.foreach(println)
  }
}
