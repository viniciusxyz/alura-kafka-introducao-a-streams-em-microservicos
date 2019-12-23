package br.com.viniciusxyz.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumer consumer = new KafkaConsumer(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));

        while(true)
        {
            var records = consumer.poll(Duration.ofMillis(100));

            if(!records.isEmpty())
            {
                System.out.println("Encontrei " + records.count());
            }

            for(Object record : records){

                ConsumerRecord localRecord = (ConsumerRecord<String, String>) record;
                System.out.println("---------------------");
                System.out.println("Sending E-mail");
                System.out.println(localRecord.key());
                System.out.println(localRecord.value());
                System.out.println(localRecord.partition());
                System.out.println(localRecord.offset());
                Thread.sleep(1000);
                System.out.println(" E-mail sent");
            }
        }
    }

    private static Properties properties() {

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());

        return properties;

    }

}
