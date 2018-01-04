package ru.mg.detectors

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import ru.mg.domain.fraud.FraudDetector.FraudDetectorFunction
import ru.mg.domain.payment.Payment

object Detectors {

  def frequentOutgoings(aggregateWindow: SlidingEventTimeWindows, threshold: Int): FraudDetectorFunction = {
    dataStream: DataStream[Payment] => new FrequentOutgoings(aggregateWindow, threshold).analyze(dataStream)
  }

  val frequentOutgoings: FraudDetectorFunction = frequentOutgoings(
    SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(15)), 3
  )
}
