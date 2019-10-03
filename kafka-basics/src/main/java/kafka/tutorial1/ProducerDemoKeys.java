package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServer = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for(int i =1; i<=10 ; i++) {
            String topic = "first_topic";
            String value = "ProducerDemoWithCallback_" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,key, value);
            logger.info("Key: " + key);
            //Send Data
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is sent successfully or an exception is thrown
                    if (e == null) {
                        //record successfulyl sent
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //To block .send() and make it synchronous - dont do in production
        }
        //flush data
        kafkaProducer.flush();
        //flush and close producer
        kafkaProducer.close();
    }
}
