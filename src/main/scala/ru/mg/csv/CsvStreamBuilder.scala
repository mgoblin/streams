package ru.mg.csv

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.RowCsvInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row

/**
  * Build DataStream from CSV file
  *
  * @param filePath CSV file path and name
  */
class CsvStreamBuilder(filePath: String) extends LazyLogging {

  def build(env: StreamExecutionEnvironment, fields: Array[TypeInformation[_]]): DataStream[Row] = {

    logger.debug(s"Build csv stream for $filePath")

    val path = new Path(filePath)
    val input = new RowCsvInputFormat(path, fields)
    val rowStream = env.readFile(input, filePath)

    logger.debug(s"Csv stream for $filePath build done")

    rowStream
  }
}
