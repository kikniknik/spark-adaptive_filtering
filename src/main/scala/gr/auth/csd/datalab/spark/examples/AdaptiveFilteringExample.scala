package gr.auth.csd.datalab.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.locate

object AdaptiveFilteringExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Adaptive Filtering Example")
      .config("spark.sql.parquet.filterPushdown", value=false)
      .config("spark.sql.adaptiveFilter.enabled", value = true)
      .config("spark.sql.adaptiveFilter.verbose", value = true)
      .config("spark.sql.adaptiveFilter.collectRate", value = 1000)
      .config("spark.sql.adaptiveFilter.calculateRate", value = 1000000)
      .config("spark.sql.adaptiveFilter.momentum", value = 0.3)
      .config("spark.sql.extensions", "gr.auth.csd.datalab.spark.sql.AdaptiveFilterExtensionInjector")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.option("sep", ";").option("header", "true").option("inferSchema", "true")
      .csv("src/main/resources/people.csv")
    df.show()

    val dff = df.filter(locate("B", 'name) === 1 && 'age > 30 && locate("Developer", 'job) === 1)

    dff.show()
    dff.explain()  // You should see AdaptiveFilter operator in Physical Plan.

    spark.stop()
  }
}
