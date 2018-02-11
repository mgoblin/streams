package ru.mg.streams

import java.time.ZoneId

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ru.mg.csv.CsvStreamBuilder
import ru.mg.domain.payment.Payment
import ru.mg.domain.payment.Payments._

object CsvFilePaymentsStream {
  def apply(env: StreamExecutionEnvironment, filePath: String): DataStream[Payment] ={
    val csvStreamBuilder = new CsvStreamBuilder(filePath)

    val stringInfo = createTypeInformation[String]
    val longInfo = createTypeInformation[Long]

    csvStreamBuilder.build(
      env,
      Array(
        stringInfo,
        stringInfo,
        longInfo,
        stringInfo
      )
    )
    .map(row => row.as[Payment])
    .assignAscendingTimestamps(_.date.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli)
  }
}
