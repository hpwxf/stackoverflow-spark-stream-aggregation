package demo

import org.apache.spark.sql.{Dataset, SQLContext, SparkSession, functions}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming._
import org.scalatest.flatspec.AnyFlatSpec

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.math.Ordered.orderingToOrdered

class AggregationSolution extends AnyFlatSpec {

  private val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL custom state management demo")
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

  "aggregation test" should "be ok" in {

    implicit val sqlCtx: SQLContext = spark.sqlContext
    val memoryStream = MemoryStream[(Instant, String, Int)]
    memoryStream.addData(data)
    val events = memoryStream
      .toDS()
      .toDF(columns: _*)
      .withWatermark("timestamp", "0 second")
      .as[(Instant, String, Int)]

    def truncateDay(ts: Instant): Instant = {
      ts.truncatedTo(ChronoUnit.DAYS)
    }

    def processEventGroup(
        group: String,
        events: Iterator[(Instant, String, Int)],
        state: GroupState[IntermediateState]
    ) = {
      def mergeState(events: List[Event]): Iterator[AggResult] = {
        assert(events.nonEmpty)
        var (acc_value, acc_timestamp) = state.getOption
          .map(s => (s.agg_value, s.last_timestamp))
          .getOrElse((0, Instant.EPOCH))

        val agg_results = events.flatMap { e =>
          val intermediate_day_result =
            if ( // not same day
              acc_timestamp != Instant.EPOCH &&
              truncateDay(e.timestamp) > truncateDay(acc_timestamp)
            ) {
              Seq(
                AggResult(
                  truncateDay(acc_timestamp),
                  group,
                  acc_value
                )
              )
            } else {
              Seq.empty
            }
          acc_value += e.value
          acc_timestamp = e.timestamp
          intermediate_day_result
        }

        state.setTimeoutTimestamp(state.getCurrentWatermarkMs, "1 day")
        state.update(IntermediateState(acc_timestamp, group, acc_value))
        agg_results.iterator
      }

      if (state.hasTimedOut && events.isEmpty) {
        assert(state.exists)
        state.getOption
          .map(agg_result =>
            AggResult(
              truncateDay(agg_result.last_timestamp),
              group,
              agg_result.agg_value
            )
          )
          .iterator
      } else {
        mergeState(events.map { case (timestamp, group, value) =>
          Event(timestamp, group, value)
        }.toList)
      }
    }

    // Accumulate value by group and report every day
    val computed_df = events
      .groupByKey(event => event._2)
      .flatMapGroupsWithState[IntermediateState, AggResult](
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout
      )(processEventGroup)

    computed_df.writeStream
      .option("truncate", value = false)
      .format("console")
      .outputMode("append")
      .start()
      .processAllAvailable()
  }
}

case class Event(
    timestamp: Instant,
    group: String,
    value: Int
)

case class IntermediateState(
    last_timestamp: Instant,
    group: String,
    agg_value: Int
)

case class AggResult(
    day_start: Instant,
    group: String,
    cumsum_by_day: Int
)
