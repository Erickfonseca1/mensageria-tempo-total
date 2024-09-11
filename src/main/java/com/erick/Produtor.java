package com.erick;

import com.rabbitmq.client.*;

public class Produtor {

    private static final String HOST = "localhost";
    private static final String USERNAME = "mqadmin";
    private static final String PASSWORD = "Admin123XX_";
    private static final String QUEUE_NAME = "nao-duravel-nao-persistente";  // Altere aqui para testar variações
    private static final int MESSAGE_COUNT = 1000000;

    public static void main(String[] args) throws Exception {
        // Configuração da conexão com RabbitMQ
        ConnectionFactory connFactory = createConnectionFactory();

        try (
                Connection conn = connFactory.newConnection();
                Channel channel = conn.createChannel()
        ) {
            // Declaração da fila - ajustar durável (true/false) e persistente (null/persistent)
            //boolean isDurable = false;  // false para não durável
            boolean isDurable = false; // true para durável
            channel.queueDeclare(QUEUE_NAME, isDurable, false, false, null);

            // Enviar mensagens (persistentes ou não)
            boolean isPersistent = false;  // Altere para false para mensagens não persistentes
            sendMessages(channel, isPersistent);
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

    // Envia 1 milhão de mensagens, cada uma contendo um número e timestamp
    private static void sendMessages(Channel channel, boolean isPersistent) throws Exception {
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            long timestamp = System.currentTimeMillis();
            String msg = i + "-" + timestamp;

            // Mensagem persistente ou não persistente
            if (isPersistent) {
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes());
            } else {
                channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
            }

            // Log de cada 100.000 mensagens enviadas
            if (i % 100000 == 0) {
                System.out.println("Sent message: " + msg);
            }
        }
    }
}
