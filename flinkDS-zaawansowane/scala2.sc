import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.OutputTag
import org.apache.flink.streaming.api.windowing.time.Time
import java.text.SimpleDateFormat
import scala.math.max
import org.apache.flink.api.common.eventtime._
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.common.functions.AggregateFunction

case class TripEvent(
                      event_type: Integer,
                      event_id: Integer,
                      vendor_id: Integer,
                      datetime: String,
                      longitude: Double,
                      latitude: Double,
                      payment_type: Integer,
                      trip_distance: Double,
                      passenger_count: Integer,
                      tolls_amount: Double,
                      tip_amount: Double,
                      total_amount: Double)

case class TripDetails(
                        startDatetime: String,
                        duration: Long,
                        distance: Double,
                        passengerCount: Integer,
                        totalAmount: Double)

case class TripDetailsAgg(
                           startDatetime: String,
                           howMany: Integer,
                           duration: Long,
                           distance: Double,
                           passengerCount: Integer,
                           totalAmount: Double)

case class TripAnalysisResult(
                               startDatetime: String,
                               howMany: Integer,
                               avgSpeed: Double,
                               passengerCount: Integer,
                               totalAmount: Double)

val env = StreamExecutionEnvironment.getExecutionEnvironment
env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0))

val tripEvents = List(
  TripEvent(0, 1, 1, "2024-01-01 08:00:00", 0.0, 0.0, 1, 10.0, 1, 0.0, 0.0, 20.0),
  TripEvent(1, 1, 1, "2024-01-01 08:30:00", 0.0, 0.0, 1, 10.0, 1, 0.0, 0.0, 20.0),
  TripEvent(0, 2, 1, "2024-01-01 08:15:00", 0.0, 0.0, 1, 15.0, 2, 0.0, 0.0, 30.0),
  TripEvent(1, 2, 1, "2024-01-01 08:45:00", 0.0, 0.0, 1, 15.0, 2, 0.0, 0.0, 30.0)
)

val tripEventsDS: DataStream[TripEvent] = env.fromCollection(tripEvents)

def getTimestamp(s: String): Long = {
  val format = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
  format.parse(s).getTime
}

def getTimestampExtended[T](t: T): Long = {
  val timeStr = t match {
    case te: TripEvent => te.datetime
    case td: TripDetails => td.startDatetime
  }
  getTimestamp(timeStr)
}

class MyTimestampAssigner[T] extends TimestampAssigner[T] {
  override def extractTimestamp(t: T, previousElementTimestamp: Long): Long = {
    getTimestampExtended[T](t)
  }
}

class MyWatermarkGenerator[T] extends WatermarkGenerator[T] {
  val maxOutOfOrderness = 6000L // 1 minute = 60000L
  var currentMaxTimestamp: Long = _
  def onEvent(t: T, previousElementTimestamp: Long, output: WatermarkOutput): Unit = {
    currentMaxTimestamp = max(getTimestampExtended[T](t), currentMaxTimestamp)
  }
  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }
}

class MyWatermarkStrategy[T] extends WatermarkStrategy[T] {
  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context) =
    new MyTimestampAssigner()
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context) =
    new MyWatermarkGenerator()
}

val wTaWTripEventsDS: DataStream[TripEvent] = tripEventsDS.assignTimestampsAndWatermarks(new MyWatermarkStrategy())

val tripDetailsDS: DataStream[TripDetails] = wTaWTripEventsDS
  .keyBy(_.event_id)
  .window(TumblingEventTimeWindows.of(Time.hours(1)))
  .apply { (key, window, events, out: Collector[TripDetails]) =>
    val start = events.find(_.event_type == 0).get
    val stop = events.find(_.event_type == 1).get
    val duration = (getTimestamp(stop.datetime) - getTimestamp(start.datetime)) / 1000
    out.collect(TripDetails(start.datetime, duration, stop.trip_distance, stop.passenger_count, stop.total_amount))
  }

class MyAggFun extends AggregateFunction[TripDetails, TripDetailsAgg, TripAnalysisResult] {
  override def createAccumulator(): TripDetailsAgg =
    TripDetailsAgg("999999999999", 0, 0, 0, 0, 0)
  override def add(value: TripDetails, accumulator: TripDetailsAgg): TripDetailsAgg =
    TripDetailsAgg(
      if (value.startDatetime < accumulator.startDatetime) value.startDatetime else accumulator.startDatetime,
      accumulator.howMany + 1,
      accumulator.duration + value.duration,
      accumulator.distance + value.distance,
      accumulator.passengerCount + value.passengerCount,
      accumulator.totalAmount + value.totalAmount)
  override def getResult(accumulator: TripDetailsAgg): TripAnalysisResult =
    TripAnalysisResult(
      accumulator.startDatetime,
      accumulator.howMany,
      if (accumulator.duration != 0) accumulator.distance / (accumulator.duration / 3600.0) else 0,
      accumulator.passengerCount,
      accumulator.totalAmount)
  override def merge(a: TripDetailsAgg, b: TripDetailsAgg): TripDetailsAgg =
    TripDetailsAgg(
      if (a.startDatetime < b.startDatetime) a.startDatetime else b.startDatetime,
      a.howMany + b.howMany,
      a.duration + b.duration,
      a.distance + b.distance,
      a.passengerCount + b.passengerCount,
      a.totalAmount + b.totalAmount)
}

val wTaWTripDetailsDS = tripDetailsDS.assignTimestampsAndWatermarks(new MyWatermarkStrategy[TripDetails]())

val finalDS = wTaWTripDetailsDS
  .windowAll(TumblingEventTimeWindows.of(Time.minutes(15)))
  .aggregate(new MyAggFun())

finalDS.print()
env.execute("Avg Speed and other stats")
