import org.apache.spark.{SparkConf, SparkContext}

object App3 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App3").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data1_" + args(0) +".csv")
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt

      (k, v)
    })
    val sum = rows.groupBy(_._1).map { case (k, l) => k -> l.map(_._2).sum }
    val extendedSum = sum.map{ case (x) => x._1 * (2 to 100).foldLeft(0){case (acc, i) => acc + x._2 % i}}.collect

    /*
    val extendedSum = sum.flatMap{ case (x) => (2 to 100).map(i => x._1 -> x._2 % i)}.collect
    */
    //sextendedSum.foreach{case (x1, x2) => println("1: " + x1 + " 2: " + x2)}
    val res = extendedSum.sum
    println("Result = " + res)
  }
}