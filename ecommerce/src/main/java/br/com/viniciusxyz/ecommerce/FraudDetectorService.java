package br.com.viniciusxyz.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FraudDetectorService {

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumer consumer = new KafkaConsumer(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        while(true)
        {
            var records = consumer.poll(Duration.ofMillis(100));

            if(!records.isEmpty())
            {
                System.out.println("Encontrei " + records.count());
            }

            for(Object record : records){

                ConsumerRecord  localRecord = (ConsumerRecord<String, String>) record;
                System.out.println("---------------------");
                System.out.println("Processing new order, checking for Fraud");
                System.out.println(localRecord.key());
                System.out.println(localRecord.value());
                System.out.println(localRecord.partition());
                System.out.println(localRecord.offset());
                Thread.sleep(5000);
            }
        }
    }

    private static Properties properties() {

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        return properties;

    }

}
