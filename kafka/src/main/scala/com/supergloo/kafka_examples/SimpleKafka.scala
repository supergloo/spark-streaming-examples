package com.supergloo.kafka_examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SimpleKafka {

  val localLogger = Logger.getLogger("SimpleKafka")

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("spark-streaming-kafka-simple")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKERS)
      .option("subscribe", INPUT_TOPIC)
      .load()

    inputDf.printSchema()

    spark.streams.awaitAnyTermination()
  }

}
