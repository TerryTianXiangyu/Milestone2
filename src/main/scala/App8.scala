import org.apache.spark.{SparkConf, SparkContext}

object App8 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App8").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data2_" +args(0)+ ".csv")

    // First change : change map to mapPartitions (with preserving partitioning)
    val rows = lines.mapPartitions(s => {
      val columns = s.map(_.split(",")).map(x => (x(0).toInt, x(1).toInt))

      columns
    }, true)

    // Second change : Change the groupBy  + map to reduceByKey (less shuffling around as it starts reducing
    // before sending results)
    val sum = rows.reduceByKey(_ + _)

    // Third change : change map to mapParitions (but without preserving partioning, as
    // data seems skewed and tended to all be on one executor)
    val sumvals = sum.mapPartitions(x => x.map(_._2), false)

    val mid = sumvals.reduce(Math.max(_, _))/2
    val maxLT = sumvals.filter(_ < mid).reduce(Math.max(_, _))
    val minGT = sumvals.filter(_ > mid).reduce(Math.min(_, _))
    println(s"MAX = $maxLT, Min = $minGT")
  }
}