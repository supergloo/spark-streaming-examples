package com.supergloo.kafka_examples

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._


object ConfluentCloud {

  val SCHEMA_REG_USER = sys.env("SCHEMA_REG_USER")
  val SCHEMA_REG_PW = sys.env("SCHEMA_REG_PW")
  val SCHEMA_REG_BASE_URL = "https://psrc-4rw99.us-central1.gcp.confluent.cloud"
  val CCLOUD_USER  = sys.env("CCLOUD_USER")
  val CCLOUD_PW = sys.env("CCLOUD_PW")
  val CLOUD_BOOTSTRAP = "pkc-43n10.us-central1.gcp.confluent.cloud:9092"

  var restService = new RestService(SCHEMA_REG_BASE_URL)

  val props = Map(
    "basic.auth.credentials.source" -> "USER_INFO",
    "schema.registry.basic.auth.user.info" -> s"${SCHEMA_REG_USER}:${SCHEMA_REG_PW}"
  ).asJava

  val schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100, props)
  private val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

  private val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(INPUT_TOPIC_AVRO + "-value").getSchema
  private var jsonS = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("spark-streaming-kafka-ccloud")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("*** AVRO  ***")

    val inputAvroDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", CLOUD_BOOTSTRAP)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username='${CCLOUD_USER}' password='${CCLOUD_PW}';")
      .option("subscribe", INPUT_TOPIC_AVRO)
      .option("startingOffsets", "earliest") // going to replay from the beginning each time
      .load()

    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes)
    )

    val valueDataFrame = inputAvroDf.selectExpr("""deserialize(value) AS message""")

    valueDataFrame.writeStream
      .outputMode("append")
      .format("console")
      .start()

    val avroDf = inputAvroDf.select(
      callUDF("deserialize", 'value).as("message")
    )
      .select(from_json('message, jsonS.dataType).as("cricket"))
      .select("cricket.*")

    avroDf.writeStream
      .outputMode("append")
      .format("console")
      .start()

//    jsonDf.printSchema()

    spark.streams.awaitAnyTermination()

  }

  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val value = super.deserialize(bytes)
      value match {
        case str: String =>
          str
        case _ =>
          val genericRecord = value.asInstanceOf[GenericRecord]
          genericRecord.toString
      }
    }
  }

}


