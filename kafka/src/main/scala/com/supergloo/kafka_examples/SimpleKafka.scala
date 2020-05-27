package com.supergloo.kafka_examples

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.types.{DataTypes, StructType}

object SimpleKafka {

  private val schemaRegistryClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY, 128)
  private val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

  private val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(INPUT_TOPIC_AVRO + "-value").getSchema
  private var jsonS = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("spark-streaming-kafka-simple")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKERS)
      .option("subscribe", INPUT_TOPIC_CSV)
      .option("startingOffsets", "earliest") // going to replay from the beginning each time
      .load()

    println("CSV")
    val csvDF = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    csvDF.writeStream
      .outputMode("append")
      .format("console")
      .start()

    // input topic 2 - from json to value object
    println("JSON")

    val structCricket = new StructType()
      .add("inning", DataTypes.IntegerType)
      .add("team", DataTypes.StringType)
      .add("delivery", DataTypes.FloatType)
      .add("batsman", DataTypes.StringType)
      .add("bowler", DataTypes.StringType)
      .add("non_striker", DataTypes.StringType)
      .add("runs", DataTypes.IntegerType)

    val inputJsonDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKERS)
      .option("subscribe", INPUT_TOPIC_JSON)
      .option("startingOffsets", "earliest") // going to replay from the beginning each time
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", structCricket).as("cricket"))


    val selectDf = inputJsonDf.selectExpr("cricket.inning", "cricket.batsman", "cricket.runs")
    selectDf.writeStream
      .outputMode("append")
      .format("console")
      .start()


    println("*** AVRO  ***")

    // with schema registry
    // https://stackoverflow.com/questions/48882723/integrating-spark-structured-streaming-with-the-confluent-schema-registry

    val inputAvroDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKERS)
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

    // Question for you...
    // the three arg `from_avro` function is only available with Databricks? re:
    // https://databricks.com/blog/2019/02/15/how-to-work-with-avro-kafka-and-schema-registry-in-databricks.html
    // let me know

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


