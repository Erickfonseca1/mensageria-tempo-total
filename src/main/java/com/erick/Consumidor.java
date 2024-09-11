package com.erick;

import com.rabbitmq.client.*;

public class Consumidor {

    private static final String HOST = "localhost";
    private static final String USERNAME = "mqadmin";
    private static final String PASSWORD = "Admin123XX_";
    private static final String QUEUE_NAME = "nao-duravel-nao-persistente";  // Altere aqui para testar variações
    private static final String RESULT_QUEUE_NAME = "result-queue";

    public static void main(String[] args) throws Exception {
        // Configuração da conexão com RabbitMQ
        ConnectionFactory connFactory = createConnectionFactory();

        try (
                Connection conn = connFactory.newConnection();
                Channel channel = conn.createChannel()
        ) {
            // Declaração da fila original e da fila de resultado
            boolean isDurable = false;  // Ajuste conforme necessário (true/false)
            channel.queueDeclare(QUEUE_NAME, isDurable, false, false, null);
            channel.queueDeclare(RESULT_QUEUE_NAME, false, false, false, null);

            // Definindo o callback do consumidor
            DeliverCallback callback = (consumerTag, delivery) -> {
                String msg = new String(delivery.getBody());
                try {
                    processMessage(msg, channel);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                // Acknowledge após processamento
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            };

            // Consumir mensagens da fila
            channel.basicConsume(QUEUE_NAME, false, callback, consumerTag -> {});
            System.out.println("Consumidor iniciado e aguardando mensagens...");

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

    // Processa a mensagem recebida e encaminha mensagens 1 e 1 milhão para nova fila
    private static void processMessage(String msg, Channel channel) throws Exception {
        String[] parts = msg.split("-");
        String numberIteration = parts[0];
        long sentTimestamp = Long.parseLong(parts[1]);

        long currentTimestamp = System.currentTimeMillis();
        long latency = currentTimestamp - sentTimestamp;

        System.out.println("Received msg: " + numberIteration + " - Latency: " + latency + " ms");

        // Reencaminhar mensagens 1 e 1 milhão para nova fila
        if (numberIteration.equals("1") || numberIteration.equals("1000000")) {
            channel.basicPublish("", "result-queue", null, msg.getBytes());
            System.out.println("Message " + numberIteration + " forwarded to result queue.");
        }
    }

    // Manter o programa rodando
    private static void keepApplicationRunning() throws InterruptedException {
        synchronized (Consumidor.class) {
            Consumidor.class.wait();
        }
    }
}