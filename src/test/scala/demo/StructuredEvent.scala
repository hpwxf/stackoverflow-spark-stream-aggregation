package demo

import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming._
import org.scalatest.flatspec.AnyFlatSpec

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.math.Ordered.orderingToOrdered

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
      val base_value = index / 2
      val ts = Instant
        .parse("2020-01-01T00:00:00Z")
        .plus(Duration.ofHours(12 * (index / 2)))
      val value = (3 - 2 * group_id) * base_value
      //      Thread.sleep(100L) // to avoid any accelerated calls on restart
      if (base_value > 11 && group_id == 2) {
        Seq.empty
      } else {
        Seq(("Group" + group_id.toString, ts, value))
      }
    }

    val inputStream =
      new MemoryStream[(String, Instant, Int)](1, spark.sqlContext)
    val events = inputStream
      .toDS()
      .toDF(columns: _*)
      .withWatermark("timestamp", "0 second")
      .as[(String, Instant, Int)]

    def processEventGroup(
        group: String,
        events: Iterator[(String, Instant, Int)],
        state: GroupState[IntermediateState]
    ) = {
      def mergeState(events: List[Event]): Iterator[AggResult] = {
        assert(events.nonEmpty)

        var (acc_value, acc_timestamp, acc_last_value) = state.getOption
          .map(s => (s.agg_value, s.last_timestamp, s.last_value))
          .getOrElse((0, Instant.EPOCH, 0))

        val agg_results = events.flatMap { e =>
          println(e)
          assert(e.group == group)
          assert(e.timestamp >= acc_timestamp) // check good data ordering
          val intermediate_day_agg =
            if (
              acc_timestamp != Instant.EPOCH &&
              e.timestamp.truncatedTo(ChronoUnit.DAYS) > acc_timestamp
                .truncatedTo(ChronoUnit.DAYS)
            ) {
              Seq(
                AggResult(
                  group,
                  acc_timestamp.truncatedTo(ChronoUnit.DAYS),
                  acc_value,
                  acc_last_value
                )
              )
            } else {
              Seq.empty
            }
          acc_value += e.value
          acc_timestamp = e.timestamp
          acc_last_value = e.value
          intermediate_day_agg
        }

        state.setTimeoutTimestamp(
          state.getCurrentWatermarkMs,
          "1 day"
        ) // or end of the last known day
        println(
          s"Updated state for $group will expire 1 day after current watermark is " +
            s"${Instant.ofEpochMilli(state.getCurrentWatermarkMs())}"
        )
        // state.remove() // when value becomes zero

        state.update(
          IntermediateState(group, acc_timestamp, acc_value, acc_last_value)
        )
        agg_results.iterator
      }

      if (state.hasTimedOut && events.isEmpty) {
        assert(state.exists)
        println(
          s"State for $group expired / current watermark is ${Instant
            .ofEpochMilli(state.getCurrentWatermarkMs())}"
        )
        val agg_result = state.get
        val end_of_the_day = agg_result.last_timestamp.truncatedTo(
          ChronoUnit.DAYS
        ) // show the beginning of the day window
        Iterator.single(
          AggResult(
            group + "_",
            end_of_the_day,
            agg_result.agg_value,
            agg_result.last_value
          )
        )
      } else {
        mergeState(events.map { case (group, timestamp, value) =>
          Event(group, timestamp, value)
        }.toList)
      }
    }

    // Accumulate value by group and report every day
    // https://stackoverflow.com/questions/63917648/spark-streaming-understanding-timeout-setup-in-mapgroupswithstate
    val eventUpdates = events
      .groupByKey(event => event._1)
      .flatMapGroupsWithState[IntermediateState, AggResult](
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout
      )(processEventGroup)

    // Start running the query that prints the session updates to the console
    val query = eventUpdates.writeStream
      .outputMode("append")
      .foreachBatch { (ds: Dataset[_], id: Long) =>
        println(s"Batch #$id")
        ds.withColumn(
          "Expected",
          (functions.abs($"last_value") * (functions
            .abs($"last_value") + lit(1))) / lit(2).as[Int]
        ).show(truncate = false)
      }
      .start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        while (!query.isActive) {}
        var index: Int = 0
        while (true) {
          Thread.sleep(300L)
          inputStream.addData(generateRow(index))
          index += 1
        }
      }
    }).start()

    query.awaitTermination(40000)
    query.recentProgress.foreach(p => {
      println(s"\nBatch #${p.batchId} (${p.batchDuration}ms)")
      p.stateOperators.foreach(s => {
        println(s.toString())
      })
    })
  }
}

case class Event(
    group: String,
    timestamp: Instant,
    value: Int
)

case class IntermediateState(
    group: String,
    last_timestamp: Instant,
    agg_value: Int,
    last_value: Int
)

// group is only for debugging purpose
case class AggResult(
    group: String,
    timestamp: Instant,
    agg_value: Int,
    last_value: Int
)
