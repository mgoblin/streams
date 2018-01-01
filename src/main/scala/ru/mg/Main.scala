package ru.mg

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import ru.mg.payment.{Payment, Person}

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info("Starting")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socketStream = env.fromElements(
      Payment(
        from = Person("Mike"),
        to = Person("Elly"),
        amount = 100
      ),
      Payment(
        from = Person("Jack"),
        to = Person("Mike"),
        amount = 150
      )
    )

    socketStream
      .addSink(s => logger.info(s"$s"))

    env.execute("Socket reader")

    logger.info("Done")

  }
}
