package br.com.viniciusxyz.ecommerce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        KafkaProducer producer = new KafkaProducer(properties());

        String key = UUID.randomUUID().toString();

        String mensagem = "123";

        ProducerRecord producerRecordOrder = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER",key,mensagem );

        producer.send(producerRecordOrder, (data, exp) -> {
            if(exp != null)
            {
                exp.printStackTrace();
                return;
            }
            System.out.println("Sucesso, mensagem enviada -- Tópico: " +  data.topic() + " - Partição: " + data.partition() + " - Offset: " + data.offset() + " - Timestamp " + data.timestamp());
        }).get();



        ProducerRecord producerRecordEmail = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL",key,mensagem );

        producer.send(producerRecordEmail, (data, exp) -> {
            if(exp != null)
            {
                exp.printStackTrace();
                return;
            }
            System.out.println("Sucesso, mensagem enviada -- Tópico: " +  data.topic() + " - Partição: " + data.partition() + " - Offset: " + data.offset() + " - Timestamp " + data.timestamp());
        }).get();




    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
