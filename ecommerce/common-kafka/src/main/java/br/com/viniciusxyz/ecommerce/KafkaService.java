package br.com.viniciusxyz.ecommerce;

import lombok.Builder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaService<T> {

    private final KafkaConsumer consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupConsumer, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> overrideProperties) {
        this(groupConsumer, parse, type, overrideProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupConsumer, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> overrideProperties) {
        this(groupConsumer, parse, type, overrideProperties);
        consumer.subscribe(topic);
    }

    public KafkaService(String groupConsumer, ConsumerFunction parse, Class<T> type, Map<String, String> overrideProperties) {
        this.parse = parse;
        consumer = new KafkaConsumer<String, T>(properties(groupConsumer, type, overrideProperties));
    }

    public void run() {

        while(true)
        {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));

            if(!records.isEmpty())
            {
                System.out.println("Encontrei " + records.count());
            }

            for(Object record : records){
                ConsumerRecord localRecord = (ConsumerRecord<String, T>) record;
                parse.consume(localRecord);
            }
        }
    }


    private Properties properties(String groupConsumer, Class type, Map<String, String> overrideProperties) {

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "140.6.254.242:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupConsumer);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        if(overrideProperties  != null )
        {
            // Sobreescreve os valores padrões das propriedades
            properties.putAll(overrideProperties);
        }

        return properties;

    }
}
