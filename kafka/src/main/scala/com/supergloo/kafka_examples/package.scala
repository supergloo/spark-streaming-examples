package com.supergloo

package object kafka_examples {

  val CHECKPOINT_LOCATION = "/tmp"

  val SCHEMA_REGISTRY = "http://localhost:8081"
  val BROKERS = "localhost:19092,localhost:29092" // running docker example locally
                                                  // https://github.com/tmcgrath/docker-for-demos/tree/master/confluent-3-broker-cluster

  val INPUT_TOPIC_CSV = "cricket_csv"
  val INPUT_TOPIC_JSON = "cricket_json"
  val INPUT_TOPIC_AVRO = "cricket_avro"

  val OUTPUT_TOPIC = "TBD"
}

