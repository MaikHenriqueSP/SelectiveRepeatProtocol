package com.chatapp;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Console;
import java.io.IOError;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.TimeUnit;

import com.chatapp.Message.MessageType;


public final class Sender implements AutoCloseable{

    /**
     *  Sets the default package size for transfer, which is by default 8 Kb.
     */
    private final Console keyboardReader;
    public static final int PACKET_TRANSFER_SIZE = 1024 * 8;
    private static int TIMER_REPLY_UDP = (int)TimeUnit.SECONDS.toMillis(2);
    private static int MAX_SERVER_RETRIES = 5;
    private final int receiverPort;
    private final int listenerPort;
    private final String receiverIpAddress;
    private final DatagramSocket udpSocket;
    private final UdpMessageListenerThread udpMessageListenerThread;


    public Sender() throws IOException {
        this.keyboardReader = System.console();

        System.out.println("Configurando receiver alvo");
        System.out.println("Digite o endereço IP do receiver:");
        this.receiverIpAddress = keyboardReader.readLine();

        System.out.println("Digite a porta do receiver:");
        this.receiverPort = Integer.parseInt(keyboardReader.readLine());

        System.out.println("Digite a porta ouvinte:");
        this.listenerPort = Integer.parseInt(keyboardReader.readLine());

        this.udpSocket = new DatagramSocket(listenerPort);
        this.udpMessageListenerThread =  new UdpMessageListenerThread();
        udpMessageListenerThread.start();
    }

    @Override
    public void close() throws IOException {
        udpSocket.close();
    }

    class UdpMessageListenerThread extends Thread {

        private DatagramPacket receivedPacket;

        @Override
        public void run() {
            System.out.println("Listening for incoming UDP messages");
            while (true) {
                listenForMessages();
            }
        }

        private void listenForMessages() {
            byte[] receivedBytes = new byte[PACKET_TRANSFER_SIZE];
            receivedPacket = new DatagramPacket(receivedBytes, receivedBytes.length);
            try {
                udpSocket.receive(receivedPacket);
                handleReceivedMessage();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        private Message handleReceivedMessage() {
            byte[] receivedData = this.receivedPacket.getData();

            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(receivedData);
                ObjectInputStream inputObject = new ObjectInputStream(new BufferedInputStream(byteArrayInputStream))) {

                Message message = (Message) inputObject.readObject();
                System.out.println("\nReceived message=" + message.toString());

                return message;
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    class UdpMessageSenderThread extends Thread {

        private final String messageBody;
        private final MessageType type;

        public UdpMessageSenderThread(String messageBody, MessageType type) {
            this.messageBody = messageBody;
            this.type = type;
        }

        @Override
        public void run() {
            System.out.println("Sending message to receiver");
            Message message = new Message(type);
            message.adicionarMensagem("body", messageBody);
            Message.sendUdpMessage(message, receiverIpAddress, receiverPort, udpSocket);
            System.out.println("Message successfully sent");
        }
    }

    /**
     * Generic implementation to close any AutoCloseable instance
     *
     * @param <T>
     * @param closeableInstance
     * @return true if the connection was successfully close, false otherwise
     */
    private static <T extends AutoCloseable> boolean  closeConnection(T closeableInstance) {
        try {
            if (closeableInstance != null) {
                closeableInstance.close();
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    public void interactiveMenu() {
        while (true) {
            try {
                System.out.println("Digite a mensagem que deseja enviar:");
                String senderMessager = keyboardReader.readLine();
                UdpMessageSenderThread senderThread = new UdpMessageSenderThread(senderMessager, MessageType.PACKAGE);
                senderThread.start();
            } catch (IOError e) {
                System.err.println("Erro na captura da opção, tente novamente");
            }
        }
    }

    public static void main(String[] args) {
        try (Sender Sender = new Sender();) {
            Sender.interactiveMenu();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

}
