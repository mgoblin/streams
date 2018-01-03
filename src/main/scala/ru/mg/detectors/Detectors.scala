package ru.mg.detectors

import org.apache.flink.streaming.api.scala.DataStream
import ru.mg.domain.fraud.FraudDetector.FraudDetectorFunction
import ru.mg.domain.payment.Payment

object Detectors {

  def frequentOutgoings(windowSizeMs: Int, slideMs: Int, threshold: Int): FraudDetectorFunction = {
    dataStream: DataStream[Payment] => new FrequentOutgoings(windowSizeMs, slideMs, threshold).analyze(dataStream)
  }

  val frequentOutgoings: FraudDetectorFunction = frequentOutgoings(60000, 15000, 3)
}
