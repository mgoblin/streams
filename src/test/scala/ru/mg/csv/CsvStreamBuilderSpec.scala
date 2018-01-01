package ru.mg.csv

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import ru.mg.utils.FileUtils

@RunWith(classOf[JUnitRunner])
class CsvStreamBuilderSpec extends FlatSpec with Matchers {
  "Csv stream builder" should "build rowed data stream from csv file" in {

    val tempFile = FileUtils.createTempFile("payments.csv")


    val csvStreamBuilder = new CsvStreamBuilder(tempFile.getAbsolutePath)

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism = 1)

    val stringInfo = createTypeInformation[String]
    val longInfo = createTypeInformation[Long]

    val input = csvStreamBuilder.build(env, Array(
      stringInfo,
      stringInfo,
      longInfo
    ))

    env.execute()

    input shouldNot be(null)
  }
}
