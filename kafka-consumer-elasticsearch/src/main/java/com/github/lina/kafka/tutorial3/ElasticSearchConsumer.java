package com.github.lina.kafka.tutorial3;


import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

       // String jsonString = "{\"foo\":\"bar\"}";

        KafkaConsumer<String, String> kafkaConsumer = createConsumer("twitter_tweets");

        //poll for new data
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info("Received "+ consumerRecords.count() +" records");
            for (ConsumerRecord<String,String> consumerRecord: consumerRecords){
                //Here is where we insert Data into ElasticSearch or other target
                //logger.info("Key: "+consumerRecord.key() + ", Value: " + consumerRecord.value());
                //logger.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());

                String jsonString = consumerRecord.value();

                // Strategy one generic using kafka
                //String kafkaId = consumerRecord.topic() +"_"+consumerRecord.partition()+"_"+consumerRecord.offset();
                //Strategy two generic to Twitter data get id from json
                String id = extractIdfromTweet(jsonString);
                IndexRequest indexRequest = new IndexRequest("twitter");
                //To make idempotent consumer
                indexRequest.id(id);
                indexRequest.source(jsonString, XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
                //Just adding a wait to see the Ids returned
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Committing Offsets.....");
            kafkaConsumer.commitSync();
            logger.info("Offsets have been committed");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //close the client gracefully
       // client.close();

    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdfromTweet(String tweetJson) {
        //gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {
        Properties properties = new Properties();
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";


        //create consumer configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //Options are earliest/latest/none
        //Disable auto commit of offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        //create consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topics
        //kafkaConsumer.subscribe(Collections.singleton(topic)); //Subscribe to only one topic
        kafkaConsumer.subscribe(Arrays.asList(topic)); //Subscribe to only one topic
        return kafkaConsumer;
    }

    public static RestHighLevelClient createClient(){
        //Replace credentials accordingly
        String hostname="localhost";
        String username="";
        String password="";

        //Not required if running ES Local
        /*final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);*/

        //For Local ES
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname,9200,"http"))
        );
        return client;
    }
}
