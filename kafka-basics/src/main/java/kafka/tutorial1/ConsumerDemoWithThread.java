package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }
    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-third-java-consumer";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the cunsumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable  = new ConsumerRunnable(bootStrapServers,topic,groupId, latch);

        //start the thread
        Thread thread = new Thread(myConsumerRunnable);
        thread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdownhook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private  KafkaConsumer<String,String> kafkaConsumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        public ConsumerRunnable(String bootStrapServers, String topic, String groupId, CountDownLatch latch){
            this.latch = latch;

            //create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //Options are earliest/latest/none

            //create consumer
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            //subscribe consumer to our topics
            //kafkaConsumer.subscribe(Collections.singleton(topic)); //Subscribe to only one topic
            kafkaConsumer.subscribe(Arrays.asList(topic)); //Subscribe to only one topic
        }
        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        logger.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                        logger.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received shutdown signal!");
            } finally {
                kafkaConsumer.close();
                //Tell our main code we are done with consumer.
                latch.countDown();
            }
        }

        public void shutdown(){
            // the wakeup() method is special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            kafkaConsumer.wakeup();
        }
    }
}

