package br.com.viniciusxyz.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerFunction<String> {

    public static void main(String[] args) throws InterruptedException {

        KafkaService service =  new KafkaService(ConsumerFunction.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL", new EmailService()::consume, String.class, null);
        service.run();

    }

    @Override
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println("---------------------");
        System.out.println("Sending E-mail");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(" E-mail sent");
    }
}
