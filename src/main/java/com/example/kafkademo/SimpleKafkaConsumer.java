package com.example.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Service
@Slf4j
public class SimpleKafkaConsumer {
    @Value("${kafka.topic}")
    private String topicName;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    private Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();

    @Scheduled(initialDelay = 10000, fixedDelay = 1000)
    public void pollKafkaServer() {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            log.info("Received message: " + message);
            commitMessage.put(new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
            kafkaConsumer.commitSync(commitMessage);
            log.info("Offset committed to Kafka.");
        }
    }

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.put("group.id", zookeeperGroupId);
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("max.poll.records", "1");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        return kafkaConsumer;
    }
}
