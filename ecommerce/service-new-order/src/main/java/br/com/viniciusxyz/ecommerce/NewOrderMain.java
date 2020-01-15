package br.com.viniciusxyz.ecommerce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String mensagem = "Mensagem qualquer";

        KafkaDispatcher dispatcherOrder = new KafkaDispatcher<Order>();
        KafkaDispatcher dispatcherEmail = new KafkaDispatcher<Email>();

        String userId = UUID.randomUUID().toString();
        String orderId = UUID.randomUUID().toString();
        double amount = Math.random() * 5000 + 1;
        Order order = new Order(userId, orderId, new BigDecimal(amount));


        String email = "teste de e-mail";


            dispatcherOrder.send("ECOMMERCE_NEW_ORDER", userId, order, new NewOrderMain()::callbackGenerico).get();
            dispatcherEmail.send("ECOMMERCE_SEND_EMAIL", userId, email, new NewOrderMain()::callbackGenerico).get();



    }

    private void callbackGenerico(RecordMetadata data, Exception exp)
    {
        if (exp != null) {
            exp.printStackTrace();
            return;
        }
        System.out.println("Sucesso, mensagem enviada -- Tópico: " + data.topic() + " - Partição: " + data.partition() + " - Offset: " + data.offset() + " - Timestamp " + data.timestamp());

    }

}
