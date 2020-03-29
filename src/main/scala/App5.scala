import org.apache.spark.{SparkConf, SparkContext}

object App5 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App5").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data5_" + args(0) +".csv")
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt
      (k, v)
    })

    val sum = rows.groupBy(_._1).map{case (z) => z._1 -> z._2.foldLeft(0){case(acc, n) => acc + (2 to 50).foldLeft(0){case (acc2, i) => acc2 + (n._2 % i)}}}
    sum.collect().foreach{case (x1, x2) => println("1: " + x1 + " 2: " + x2)}

  }
}