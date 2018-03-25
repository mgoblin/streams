package ru.mg.detectors

import org.apache.flink.streaming.api.scala.DataStream
import ru.mg.domain.fraud.Fraud
import ru.mg.streams._
import org.apache.flink.api.scala._

object Detectors {

  def frequentOutgoings(threshold: Int): DataStream[OutgoingPaymentsGroup] => DataStream[Fraud] = {
    dataStream: DataStream[OutgoingPaymentsGroup] =>
      dataStream
        .filter(op => op.lengthCompare(threshold) > 0)
        .map(payments => Fraud(payments.head.fromPerson, payments, "frequent outgoing payments"))
  }

  def frequentIncomings(threshold: Int): DataStream[IncomingPaymentsGroup] => DataStream[Fraud] = {
    dataStream: DataStream[IncomingPaymentsGroup] =>
      dataStream
        .filter(op => op.lengthCompare(threshold) > 0)
        .map(payments => Fraud(payments.head.toPerson, payments, "frequent incoming payments"))
  }

  implicit class FreqUtils(stream: DataStream[OutgoingPaymentsGroup]) {
    def findFrequentOutgoingsFraud(threshold: Int): DataStream[Fraud] = frequentOutgoings(threshold)(stream)
    def finFrequentIncomingsFraud(threshold: Int): DataStream[Fraud] = frequentIncomings(threshold)(stream)
  }
}
