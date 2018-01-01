package ru.mg.csv

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.mg.payment.Payment

import ru.mg.payment.Payments._

object PaymentsStream {
  def apply(env: StreamExecutionEnvironment, filePath: String): DataStream[Payment] ={
    val csvStreamBuilder = new CsvStreamBuilder(filePath)

    val stringInfo = createTypeInformation[String]
    val longInfo = createTypeInformation[Long]

    csvStreamBuilder.build(
      env,
      Array(
        stringInfo,
        stringInfo,
        longInfo
      )
    ).map(row => Payment.convertFrom(row))
  }
}
