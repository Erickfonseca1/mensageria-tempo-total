package com.erick;

import com.rabbitmq.client.*;

public class FinalConsumer {

    private static final String HOST = "localhost";
    private static final String USERNAME = "mqadmin";
    private static final String PASSWORD = "Admin123XX_";
    private static final String RESULT_QUEUE_NAME = "result-queue";

    private static long firstTimestamp = -1;
    private static long lastTimestamp = -1;

    public static void main(String[] args) throws Exception {
        // Configuração da conexão com RabbitMQ
        ConnectionFactory connFactory = createConnectionFactory();

        try (
                Connection conn = connFactory.newConnection();
                Channel channel = conn.createChannel()
        ) {
            // Declaração da fila de resultado
            channel.queueDeclare(RESULT_QUEUE_NAME, false, false, false, null);

            // Definindo o callback do consumidor
            DeliverCallback callback = (consumerTag, delivery) -> {
                String msg = new String(delivery.getBody());
                processFinalMessage(msg, channel);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };

            // Consumir mensagens da fila
            channel.basicConsume(RESULT_QUEUE_NAME, false, callback, consumerTag -> {});
            System.out.println("Final consumer aguardando mensagens...");

            // Manter o programa rodando indefinidamente
            keepApplicationRunning();
        }
    }

    // Cria e configura a ConnectionFactory
    private static ConnectionFactory createConnectionFactory() {
        ConnectionFactory connFactory = new ConnectionFactory();
        connFactory.setHost(HOST);
        connFactory.setUsername(USERNAME);
        connFactory.setPassword(PASSWORD);
        return connFactory;
    }

    // Processa as mensagens 1 e 1 milhão para calcular o tempo total
    private static void processFinalMessage(String msg, Channel channel) {
        String[] parts = msg.split("-");
        String numberIteration = parts[0];
        long sentTimestamp = Long.parseLong(parts[1]);

        if (numberIteration.equals("1")) {
            firstTimestamp = sentTimestamp;
        } else if (numberIteration.equals("1000000")) {
            lastTimestamp = sentTimestamp;
        }

        if (firstTimestamp != -1 && lastTimestamp != -1) {
            long totalTime = lastTimestamp - firstTimestamp;
            System.out.println("Total time for 1 million messages: " + totalTime + " ms");
        }
    }

    // Manter o programa rodando
    private static void keepApplicationRunning() throws InterruptedException {
        synchronized (FinalConsumer.class) {
            FinalConsumer.class.wait();
        }
    }
}