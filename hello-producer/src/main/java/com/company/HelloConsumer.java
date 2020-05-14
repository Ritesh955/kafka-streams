package com.company;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;
import java.util.Collections;

public class HelloConsumer {
    private static final Logger logger = LogManager.getLogger(HelloConsumer.class);

    public static void main(String args[]){
        String topicName;

        if (args.length == 0) {
            System.out.println("Please provide command line argument: topicName");
            System.exit(-1);
        }
        topicName = args[0];
        System.out.println(topicName);
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-5063");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        final int giveUp = 100;   int noRecordsCount = 0;
        try {
            while (true) {
                System.out.println("Here");
                ConsumerRecords<Integer, String> consumerRecords = consumer.poll(100);
                System.out.println(consumerRecords.count());
                if (consumerRecords.count()==0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) break;
                    else continue;
                }
                consumerRecords.forEach(record -> {
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                });
                consumer.commitAsync();
            }
        } catch (KafkaException e) {
            logger.error("Exception occurred – Check log for more details.\n" + e.getMessage());
            System.exit(-1);
        } finally {
            logger.info("Finished HelloProducer – Closing Kafka Producer.");
            consumer.close();
        }
    }

}
