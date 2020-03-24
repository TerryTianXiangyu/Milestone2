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
/*
    What App 1 does
    App 1 is collecting and summing c(2) of same users representing by c(1).
    App 1 retrieves firstly useful data and store as RDD
    Each data is then transformed into tuples with corresponding values (modified or not)
    Next, all data are collected to the driver
    Afterwards, all data are grouped by the first value(key) in tuple, and values having the same key are summed
    Finally, base on the pair (key, new summed values), App 1 create a map named t2 and print all its pairs.

    Log indicates the original code has ExecutorLostFailure error which is due to OutOfMemoryError.
    "Exception in thread thread_name: java.lang.OutOfMemoryError: Java heap space" for "map at App1.scala:22"
    Container was killed on request and caused ExecutorLostFailure. There was no more enough space for executor and the task
    is too heavy for an executor in terms of memory required. Since we cannot modify configuration(i.e. increase executor/driver
    memory). We tried to create more partitions in order to reduce its size by repartition. However, OOM raised again OOM
    because of collecting large data into the driver.The codes collect first all processed data and then combine certain data
    together. This raised error when the data is huge. We can solve this problem by move "collect" to the end of operations so that
    there are less data being transferred into driver. Nevertheless, this still consume much time.
    Thus, our solution consists of more efficient way to accomplish the task. That is to reduce the results elements before calling
    "collect". We synthesize operations by reduceByKey, which replaces groupBy and map-sum operation. In this manner, data are
    largely reduced and program does not yield errors.

    Note that the final instance t2 has originally type Map[Int, Int], we add "toMap" to keep the same type.
*/
    // add more partition to RDD by "repartition"
    val t1 = rows.repartition(30).map(p => p._2 -> p._3/10)
    // original codes
    //val t2 = t1.reduceByKey(_+_).collect.toMap

    // optimized codes
    val t2 = t1.reduceByKey(_+_).collect.toMap
    t2.foreach(println)
  }
}
