package br.com.viniciusxyz.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;


public class LogService implements ConsumerFunction<String> {

    public static void main(String[] args) throws InterruptedException {
        KafkaService consumer = new KafkaService(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), new LogService()::consume, String.class, Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
        consumer.run();
    }



    @Override
    public void consume(ConsumerRecord<String, String> record) {
        System.out.println("---------------------");
        System.out.println("Log " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(" Log finish");
    }
}
