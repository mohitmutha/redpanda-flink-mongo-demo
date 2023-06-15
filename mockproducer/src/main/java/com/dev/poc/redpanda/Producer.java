package com.dev.poc.redpanda;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

public class Producer {
  private KafkaProducer<String, String> kafkaProducer;
  private static Logger logger = LoggerFactory.getLogger(Producer.class);

  public void runAlways(String topicName) throws Exception {

    while (true) {
      String key = UUID.randomUUID().toString();
      Date date = new Date();

      String message = "{\"timestamp\":" + date.getTime() + ",\"number\":" + (Math.round(Math.random() * 100)) + "}";
      logger.info("Sending message {}", message);
      // send the message
      this.send(topicName, key, message);
      logger.info("--- Sleeping for 1 sec --- ");

      Thread.sleep(1000);
    }
  }

  protected void send(String topicName, String key, String message) throws Exception {

    // create the ProducerRecord object which will
    // represent the message to the Kafka broker.
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);

    // Send the message to the Kafka broker using the internal
    // KafkaProducer
    getKafkaProducer().send(producerRecord, new Callback() {

      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
          logger.error("Exception sending message", exception);
        } else {
          logger.info(metadata.toString());
        }
      }

    });
    this.kafkaProducer.flush();
  }

  private KafkaProducer<String, String> getKafkaProducer() throws Exception {
    if (this.kafkaProducer == null) {
      Properties properties = new Properties();
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      this.kafkaProducer = new KafkaProducer<>(properties);
    }
    return this.kafkaProducer;
  }

}
