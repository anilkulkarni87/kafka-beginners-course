package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-second-java-consumer";
        String topic = "first_topic";

        //create consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //Options are earliest/latest/none

        //create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topics
        //kafkaConsumer.subscribe(Collections.singleton(topic)); //Subscribe to only one topic
        kafkaConsumer.subscribe(Arrays.asList(topic)); //Subscribe to only one topic

        //poll for new data
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> consumerRecord: consumerRecords){
                logger.info("Key: "+consumerRecord.key() + ", Value: " + consumerRecord.value());
                logger.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
            }
        }
    }
}

