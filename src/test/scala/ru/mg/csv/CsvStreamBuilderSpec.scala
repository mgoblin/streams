package ru.mg.csv

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import ru.mg.utils.{FileUtils, SinkCollector}

@RunWith(classOf[JUnitRunner])
class CsvStreamBuilderSpec extends FlatSpec with Matchers with LazyLogging with Serializable {

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

  it should "emit all csv records as Row events" in {
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

    SinkCollector.clear()
    val sinkCollector = SinkCollector[Row]

    input.addSink(e => sinkCollector.add(e))

    env.execute()

    SinkCollector.collector.size shouldEqual 2
  }
}
