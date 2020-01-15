package br.com.viniciusxyz.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> {

    private final KafkaProducer producer = new KafkaProducer<String, T>(properties());

    public Future send(String topic, String key, T mensagem, Callback callbackNewOrder) {
        ProducerRecord producerRecordOrder = new ProducerRecord<>(topic,key,mensagem );
        return producer.send(producerRecordOrder, callbackNewOrder);
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "140.6.254.242:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

}
