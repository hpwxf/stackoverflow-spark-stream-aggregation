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
      .flatMapGroupsWithState[List[SessionAcc], AggResult](
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout
      ) {
        case (
              group: String,
              events: Iterator[(String, Instant, Int)],
              state: GroupState[List[SessionAcc]]
            ) =>
          def handleEvict(sessions: List[SessionAcc]): Iterator[AggResult] = {
            // we sorted sessions by timestamp
            val (evicted, kept) = sessions.span { s =>
              s.endTime.toEpochMilli < state.getCurrentWatermarkMs()
            }

            if (kept.isEmpty) {
              state.remove()
            } else {
              state.update(kept)
              // trigger timeout at the end time of the first session
              state.setTimeoutTimestamp(kept.head.endTime.toEpochMilli)
            }

            evicted.map { sessionAcc =>
              AggResult(
                group,
                sessionAcc.endTime.toEpochMilli - sessionAcc.startTime.toEpochMilli,
                sessionAcc.events.length
              )
            }.iterator
          }

          def mergeSessions(sessions: List[SessionAcc]): Unit = {
            // we sorted sessions by timestamp
            val updatedSessions = new mutable.ArrayBuffer[SessionAcc]()
            updatedSessions ++= sessions

            var curIdx = 0
            while (curIdx < updatedSessions.length - 1) {
              val curSession = updatedSessions(curIdx)
              val nextSession = updatedSessions(curIdx + 1)

              // Current session and next session can be merged
              if (
                curSession.endTime.toEpochMilli > nextSession.startTime.toEpochMilli
              ) {
                val accumulatedEvents =
                  (curSession.events ++ nextSession.events).sortBy(
                    _.startTimestamp
                  )

                val newSessions = new mutable.ArrayBuffer[SessionAcc]()
                var eventsForCurSession =
                  new mutable.ArrayBuffer[Event]()
                accumulatedEvents.foreach { event =>
                  eventsForCurSession += event
//                  if (event.eventType == EventTypes.CLOSE_SESSION) {
//                    newSessions += SessionAcc(eventsForCurSession.toList)
//                    eventsForCurSession = new mutable.ArrayBuffer[Event]()
//                  }
                }
                if (eventsForCurSession.nonEmpty) {
                  newSessions += SessionAcc(eventsForCurSession.toList)
                }

                // replace current session and next session with new session(s)
                updatedSessions.remove(curIdx + 1)
                updatedSessions(curIdx) = newSessions.head
                if (newSessions.length > 1) {
                  updatedSessions.insertAll(curIdx + 1, newSessions.tail)
                }

                // move the cursor to the last new session(s)
                curIdx += newSessions.length - 1
              } else {
                // move to the next session
                curIdx += 1
              }
            }

            // update state
            state.update(updatedSessions.toList)
          }

          if (state.hasTimedOut && state.exists) {
            handleEvict(state.get.sortBy(_.startTime.toEpochMilli))
          } else {
            // convert each event as individual session
            val sessionsFromEvents = events.map {
              case (group, timestamp, value) =>
                val e = Event(group, timestamp, gapDuration, value)
                SessionAcc(List(e))
            }.toList
            if (sessionsFromEvents.nonEmpty) {
              val sessionsFromState = if (state.exists) {
                state.get
              } else {
                List.empty
              }

              // sort sessions via start timestamp, and merge
              mergeSessions(
                (sessionsFromEvents ++ sessionsFromState)
                  .sortBy(_.startTime)
              )
              // we still need to handle eviction here
              handleEvict(state.get.sortBy(_.startTime))
            } else {
              Iterator.empty
            }
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

case class SessionAcc(events: List[Event]) {
  private val sortedEvents: List[Event] =
    events.sortBy(_.startTimestamp)

  def eventsAsSorted: List[Event] = sortedEvents

  def startTime: Instant = sortedEvents.head.startTimestamp

  def endTime: Instant = sortedEvents.last.endTimestamp
}

case class AggResult(id: String, durationMs: Long, numEvents: Int)
