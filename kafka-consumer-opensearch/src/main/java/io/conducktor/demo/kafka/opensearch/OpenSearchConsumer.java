package io.conducktor.demo.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create consumer configs
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }

    private static String extractId(String json){
        //gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //first creat an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create our kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();


        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // we need to create the index on opensearch if it doesn't exist already
        try (openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exits");
            }

            //we subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                //if not exist data then in this line for 3000s
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();


                //send these record to elastic search
                for (ConsumerRecord<String, String> record : records) {

                    //strategy1
                    //define an ID using kafka record coordinates
                    //String id = record.topic() + "_" + record.partition() + "_" + record.offset();


                    try {
                        String id = extractId(record.value());

                        //send the record into opensearch
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);


                        //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);

                        //log.info(response.getId());
                    } catch (Exception e) {
                        log.error("Failed to process record at offset {}", record.offset(), e);
                    }
                }


                //bulkresponse
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //commit offsets after the batch is consumed
                consumer.commitSync();
                log.info("Offsets have been commited!");
            }


            //main code logic from kafka write into the opensearch

            // close thing

        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            openSearchClient.close();
            log.info("The consumer is now gracefully shut down");
        }
    }
};

