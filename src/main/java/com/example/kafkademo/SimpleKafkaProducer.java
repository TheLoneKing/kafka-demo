package com.example.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@Slf4j
public class SimpleKafkaProducer {
    @Value("${kafka.topic}")
    private String topicName;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Value("${zookeeper.host}")
    private String zookeeperHost;

    @Autowired
    private KafkaProducer<String, String> producer;

    private int index = 0;

    @Scheduled(initialDelay=5000, fixedDelay=20000)
    public void run() {
        String payload = "The index is now: " + index;
        log.info("Sending Kafka message: " + payload);
        producer.send(new ProducerRecord<>(topicName, payload));
        index++;
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        /*
         * Defining producer properties.
         */
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 16384);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /*
        Creating a Kafka Producer object with the configuration above.
         */
        return new KafkaProducer<>(producerProperties);
    }
}
