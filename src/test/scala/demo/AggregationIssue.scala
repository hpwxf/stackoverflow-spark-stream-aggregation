package demo

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.{Duration, Instant}

class AggregationIssue extends AnyFlatSpec {

  private val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL user-defined Datasets aggregation example")
    .getOrCreate()

  import spark.implicits._

  private val columns = Seq("timestamp", "group", "value")
  private val data = List(
    (Instant.parse("2020-01-01T00:00:00Z"), "Group1", 0),
    (Instant.parse("2020-01-01T00:00:00Z"), "Group2", 0),
    (Instant.parse("2020-01-01T12:00:00Z"), "Group1", 1),
    (Instant.parse("2020-01-01T12:00:00Z"), "Group2", -1),
    (Instant.parse("2020-01-02T00:00:00Z"), "Group1", 2),
    (Instant.parse("2020-01-02T00:00:00Z"), "Group2", -2),
    (Instant.parse("2020-01-02T12:00:00Z"), "Group1", 3),
    (Instant.parse("2020-01-02T12:00:00Z"), "Group2", -3),
    (Instant.parse("2020-01-03T00:00:00Z"), "Group1", 4),
    (Instant.parse("2020-01-03T00:00:00Z"), "Group2", -4),
    (Instant.parse("2020-01-03T12:00:00Z"), "Group1", 5),
    (Instant.parse("2020-01-03T12:00:00Z"), "Group2", -5)
  )

  "batch test" should "be ok" in {
    val df = spark
      .createDataFrame(data)
      .toDF(columns: _*)

    df.printSchema()

    val event_window = Window
      .partitionBy(col("group"))
      .orderBy(col("timestamp"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val computed_df = df
      .withColumn(
        "cumsum",
        functions
          .sum('value)
          .over(event_window)
      )
      .groupBy(window($"timestamp", "1 day"), $"group")
      .agg(functions.last("cumsum").as("cumsum_by_day"))

    computed_df.show(truncate = false)
    computed_df.printSchema()
  }

  "memory stream test" should "be ok" in {

    implicit val sqlCtx: SQLContext = spark.sqlContext
    val memoryStream = MemoryStream[(Instant, String, Int)]
    memoryStream.addData(data)
    val df = memoryStream
      .toDF()
      .toDF(columns: _*)

    val computed_df = df
      .groupBy(window($"timestamp", "1 day"), $"group")
      .agg(functions.sum('value).as("agg"))

    computed_df.printSchema()

    computed_df.writeStream
      .option("truncate", value = false)
      .format("console")
      .outputMode("complete")
      .start()
      .processAllAvailable()
  }
}
