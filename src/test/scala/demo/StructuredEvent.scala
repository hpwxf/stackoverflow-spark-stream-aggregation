package demo

import org.apache.spark.sql.execution.streaming.MemoryStream

import java.sql.Timestamp
import scala.collection.mutable
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.{Duration, Instant}
import scala.math.Ordered.orderingToOrdered
import scala.util.Random

/** Also note that the implementation is simplified one. This example doesn't address
  * - UPDATE MODE (the semantic is not clear for session window with event time processing)
  * - partial merge (events in session which are earlier than watermark can be aggregated)
  * - other possible optimizations
  */
class StructuredEvent extends AnyFlatSpec {

  private val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL custom state management demo")
    .getOrCreate()

  import spark.implicits._

  private val columns = Seq("group", "timestamp", "value")

  "session test" should "be ok" in {

    def generateRow(index: Int) = {
      val group_id = (index % 2) + 1
      val ts = Instant
        .parse("2020-01-01T00:00:00Z")
        .plus(Duration.ofHours(12 * (index / 2)))
      val value = (3 - 2 * group_id) * (index / 2)
      //      Thread.sleep(100L) // to avoid any accelerated calls on restart
      //      if (index > 10 && group_id == 2) {
      //        Seq.empty
      //      } else {
      Seq(("Group" + group_id.toString, ts, value))
      //      }
    }

    val events = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .flatMap(r => generateRow(r.getLong(1).toInt))
      .toDF(columns: _*)
      .withWatermark("timestamp", "10 seconds")
      .as[(String, Instant, Int)]

    val gapDuration: Duration = Duration.ofHours(24)

    // Accumulate value by group and report every day
    val sessionUpdates = events
      .groupByKey(event => event._1)
      .flatMapGroupsWithState[IntermediateState, AggResult](
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout
      ) {
        case (
              group: String,
              events: Iterator[(String, Instant, Int)],
              state: GroupState[IntermediateState]
            ) =>
          def mergeState(events: List[Event]): Iterator[AggResult] = {
            assert(events.nonEmpty)
            val (initial_value, initial_timestamp) = if (state.exists) {
              (state.get.agg_value, state.get.last_timestamp)
            } else {
              (0, Instant.ofEpochSecond(0))
            }

            var updated_value = initial_value
            var updated_timestamp = initial_timestamp

            val agg_results = events.map { e =>
              assert(e.group == group)
              assert(e.startTimestamp >= updated_timestamp)
              updated_value += e.value
              AggResult(e.group, e.startTimestamp, e.value)
            }
            state.update(
              IntermediateState(group, updated_timestamp, updated_value)
            )
            agg_results.iterator
          }

          if (state.hasTimedOut && state.exists) {
            // state.remove()
            Iterator.empty
          } else {
            mergeState(events.map { case (group, timestamp, value) =>
              Event(group, timestamp, gapDuration, value)
            }.toList)
          }
      }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

case class Event(
    group: String,
    startTimestamp: Instant,
    endTimestamp: Instant,
    value: Int
)

object Event {
  def apply(
      group: String,
      timestamp: Instant,
      gapDuration: Duration,
      value: Int
  ): Event = {
    val endTime = timestamp.plus(gapDuration) // FIXME should be end of the day
    Event(group, timestamp, endTime, value)
  }
}

case class IntermediateState(
    group: String,
    last_timestamp: Instant,
    agg_value: Int
)
case class AggResult(id: String, timestamp: Instant, agg_value: Int)
