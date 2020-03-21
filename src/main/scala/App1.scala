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
    // In our case, it should be " for a long-lived application, the message might be an indication that the application is
    // unintentionally holding references to objects, and this prevents the objects from being garbage collected."
    // Note that we are using groupBy initially. And using groupBy, we are shuffling all elements of each partition through
    // networks. But if we can combine first elements on each partition, large amount of data can be avoided being transferred.
    // Thus our optimization is that we sum first elements on the same partition, then send resulted elements over the network
    // in order to combine with other elements. This does not raise problem since addition is associative.

    // since we largely reduce amount of data being transferred, we can collect to the driver. Or else, collect can be done
    // at the end of the operation. (i.e. val t2 = t1.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum).collect) Note
    // that in our situation (compute with "large" dataset), put "collect" where it was or to the end do not change runtime
    // Thus, we decide to keep its original location due to rule: make less changes possible)

    val t1 = rows.map(p => p._2 -> p._3/10).reduceByKey(_+_)
    val t2 = t1.collect.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).sum)
    t2.foreach(println)
  }
}
