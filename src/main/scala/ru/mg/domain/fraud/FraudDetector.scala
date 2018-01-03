package ru.mg.domain.fraud

import org.apache.flink.streaming.api.scala.DataStream
import ru.mg.domain.payment.Payment

trait FraudDetector {
  def analyze(dataStream: DataStream[Payment]): DataStream[Fraud]
}

object FraudDetector {
  type FraudDetectorFunction = DataStream[Payment] => DataStream[Fraud]
}
