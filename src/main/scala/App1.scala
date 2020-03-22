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

    // can we know what is c(2) ?
    // c(1) is probably app id, c(0) is user name

    // App 1 is collecting and summing c(2) of same users representing by c(1).
    // Before summing c(2), their value are divide by ten (integer division)
    // Finally, user id and the corresponding value are both printed out.

    // c(0) is not used in our computaton codes.
    // With or without c(0) do not change runtime.
    // Since we need the minimal changes, we do not remove c(0)

    // Log indicates the original code has ExecutorLostFailure error which is due to OOM (OutOfMemoryError)
    // Exception in thread thread_name: java.lang.OutOfMemoryError: Java heap space
    // According to the documentation, "the detail message Java heap space indicates object could not be allocated in the Java heap."
    // In our case, it should be "for a long-lived application, the message might be an indication that the application is
    // unintentionally holding references to objects, and this prevents the objects from being garbage collected."
    // Note that we are using groupBy initially. And using groupBy, we are shuffling all elements of each partition through
    // networks.
    // Thus our optimization is to replace groupBy and map by reduceByKey, we combine firstly some elements on each partition, reduce the elements
    // being transferred over the network. Since App 1 sums over all values after "groupBy", and addition is associative,
    // we simply use addition function for the parameter of reduceByKey.

    // Note that the final instance t2 has originally type Map[Int, Int], we add "toMap" to keep the same type.

    val t1 = rows.map(p => p._2 -> p._3/10)
    // original codes
    //val t2 = t1.collect.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum)

    // optimized codes
    val t2 = t1.reduceByKey(_+_).collect.toMap

    t2.foreach(println)
  }
}
