package br.com.viniciusxyz.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService implements ConsumerFunction<Order> {

    public static void main(String[] args) throws InterruptedException {
        KafkaService service = new KafkaService<Order>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", new FraudDetectorService()::consume, Order.class, null);
        service.run();
    }

    @Override
    public void consume(ConsumerRecord<String, Order> record) {
        System.out.println("---------------------");
        System.out.println("Processing new order, checking for Fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
