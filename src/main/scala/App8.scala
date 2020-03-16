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
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt
      (k, v)
    })
    val sum = rows.groupBy((r: (Int, Int)) => r._1).map{case (k, l) => k -> l.map(_._2).sum}
    val sumvals = sum.map(_._2)
    val mid = sumvals.reduce(Math.max(_, _))/2
    val maxLT = sumvals.filter(_ < mid).reduce(Math.max(_, _))
    val minGT = sumvals.filter(_ > mid).reduce(Math.min(_, _))
    println(s"MAX = $maxLT, Min = $minGT")
  }
}