import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.OutputTag


val no_of_retries = 3
val interval = 10000
val localFsURI = "file:///tmp/flink-input"
senv.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(no_of_retries,0))
val filePath = new Path("*.csv")
val format = new TextInputFormat(filePath)
format.setFilesFilter(FilePathFilter.createDefaultFilter())
val inputStream = senv.readFile(format, localFsURI,
  FileProcessingMode.PROCESS_CONTINUOUSLY, interval)

case

class TripEvent(
                 event_type:Integer,
                 event_id:Integer,
                 vendor_id:Integer,
                 datetime:String,
                 longitude:Double,
                 latitude:Double,
                 payment_type:Integer,
                 trip_distance:Double,
                 passenger_count:Integer,
                 tolls_amount:Double,
                 tip_amount:Double,
                 total_amount:Double)

case

class TripDetails(startDatetime:String,
                  duration:Long,
                  distance:Double,
                  passengerCount:Integer,
                  totalAmount:Double)
case

class TripDetailsAgg(startDatetime:String,
                     howMany:Integer,
                     duration:Long,
                     distance:Double,
                     passengerCount:Integer,
                     totalAmount:Double)
case

class TripAnalysisResult(startDatetime:String,
                         howMany:Integer,
                         avgSpeed:Double,
                         passengerCount:Integer,
                         totalAmount:Double)

val tripEventsDS: org.apache.flink.streaming.api.scala.DataStream[TripEvent] =
  inputStream.filter(s => !s.startsWith("event_type"))
    .map(_.split(","))
    .filter(_.size == 12)
    .map(array => TripEvent(
      array(0).toInt, array(1).toInt,
      array(2).toInt, array(3).toString,
      array(4).toDouble, array(5).toDouble,
      array(6).toInt, array(7).toDouble,
      array(8).toInt, array(9).toDouble,
      array(10).toDouble, array(11).toDouble))


import java.text.SimpleDateFormat
def getTimestamp(s: String) : Long = {
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

import org.apache.flink.api.common.eventtime._
class MyTimestampAssigner[T] extends TimestampAssigner[T] {
  override def extractTimestamp(t: T, previousElementTimestamp: Long): Long = {
    getTimestampExtended[T](t)
  }
}

import scala.math.max
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

val wTaWTripEventsDS: DataStream[TripEvent] = tripEventsDS.
  assignTimestampsAndWatermarks(new MyWatermarkStrategy())

import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.ProcessFunction
val startTag = OutputTag[TripEvent]("start")


class StartStopSplitter extends ProcessFunction[TripEvent, TripEvent] {
  override def processElement(value: TripEvent,
                              ctx: ProcessFunction[TripEvent, TripEvent]#Context,
                              out: Collector[TripEvent]) {
    (value.event_type % 2) match {
      case 0 => // send start TripEvents to a side output
        ctx.output(startTag, value)
      case 1 => // emit stop TripEvents to regular output
        out.collect(value)
    }
  }
}

val wTaWStopTEDS = wTaWTripEventsDS.process(new StartStopSplitter())
val wTaWStartTEDS: DataStream[TripEvent] = wTaWStopTEDS.getSideOutput(startTag)

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
val joinedTEDS:
  org.apache.flink.streaming.api.scala.DataStream[TripDetails] = wTaWStartTEDS.join(wTaWStopTEDS).
  where(_.event_id).equalTo(_.event_id).
  window(TumblingEventTimeWindows.of(Time.hours(1))){
  (start, stop) => TripDetails(
    start.datetime,
    (getTimestamp(stop.datetime)-getTimestamp(start.datetime))/1000,
    stop.trip_distance,
    stop.passenger_count,
    stop.total_amount
  )}


import org.apache.flink.api.common.functions.AggregateFunction
class MyAggFun extends AggregateFunction[TripDetails,TripDetailsAgg,TripAnalysisResult] {
  override def createAccumulator(): TripDetailsAgg =
    TripDetailsAgg("999999999999", 0, 0, 0, 0, 0)
  override def add(value: TripDetails,
                   accumulator: TripDetailsAgg): TripDetailsAgg =
    TripDetailsAgg(if (value.startDatetime < accumulator.startDatetime) value.startDatetime
    else accumulator.startDatetime,
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
      accumulator.totalAmount
    )
  override def merge(a: TripDetailsAgg,
                     b: TripDetailsAgg): TripDetailsAgg =
    TripDetailsAgg(if (a.startDatetime < b.startDatetime) a.startDatetime
    else b.startDatetime,
      a.howMany + b.howMany,
      a.duration + b.duration,
      a.distance + b.distance,
      a.passengerCount + b.passengerCount,
      a.totalAmount + b.totalAmount)
}

val wTaWJoinedTEDS = joinedTEDS.
  assignTimestampsAndWatermarks(new MyWatermarkStrategy[TripDetails]())

val finalDS = wTaWJoinedTEDS.
  windowAll(TumblingEventTimeWindows.of(Time.minutes(15))).
  aggregate(new MyAggFun())

finalDS.print
senv.execute("Avg Speed and other stats")
