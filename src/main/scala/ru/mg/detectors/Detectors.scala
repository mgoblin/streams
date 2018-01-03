package ru.mg.detectors

object Detectors {
  import ru.mg.domain.fraud.FraudDetector._

  val frequentOutgoings: FraudDetector = dataStream =>
    new FrequentOutgoings().analyze(dataStream)
}
