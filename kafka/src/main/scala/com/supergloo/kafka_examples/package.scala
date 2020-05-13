package com.supergloo

package object kafka_examples {

  val CHECKPOINT_LOCATION = "/tmp"

  val BROKERS = "localhost:19092,localhost:29092" // running docker example locally
  val INPUT_TOPIC = "blah"
  val INPUT_TOPIC_2 = "blah2"

  val OUTPUT_TOPIC = "blah"
}

