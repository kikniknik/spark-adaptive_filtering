package gr.auth.csd.datalab.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.length

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

    // Following dataframe is a tiny sample of 1-gram dataset.
    // Find it here: https://storage.googleapis.com/books/ngrams/books/datasetsv2.html
    val df = Seq(("Chaiton",1991,1,1), ("Alcazaba",1962,3,2), ("BRACHIAL",1856,16,16))
      .toDF("gram", "year", "times", "books")

    val dff = df.filter(length('gram) < 4 && 'books > 0 && 'year > 1900 && 'times > 200)

    println(dff.count)
    dff.explain()  // You should see AdaptiveFilter operator in Physical Plan.

    spark.stop()
  }
}
