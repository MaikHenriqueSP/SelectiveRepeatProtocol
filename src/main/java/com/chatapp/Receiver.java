package com.chatapp;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.Charset;

import com.chatapp.Message.MessageType;

public class Receiver implements AutoCloseable {
    private final DatagramSocket socketUDP;
    public static final int SOCKET_RECEIVED_PORT = 10098;
    public static final String SERVER_ADDRESS = "localhost";
    public final String FICTITIOUS_SERVER_ADDRESS;
    private static final int TRANSFER_PACKET_SIZE = 8 * 1024;

    public Receiver(String serverAddress) throws IOException {
        this.socketUDP = new DatagramSocket(SOCKET_RECEIVED_PORT);
        this.FICTITIOUS_SERVER_ADDRESS = serverAddress;
    }

    /**
     * Server listener, which means it remains blocked waiting on UDP messages.
     * On every message received it starts a thread responsible for dealing with the request, allowing the server to handle multiple requests.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void listenForMessages() throws IOException, ClassNotFoundException {
        while (true) {
            byte[] receivedBytes = new byte[TRANSFER_PACKET_SIZE];
            DatagramPacket packet = new DatagramPacket(receivedBytes, receivedBytes.length);
            socketUDP.receive(packet);
            new ListenerThread(packet).start();
        }
    }

    @Override
    public void close() throws Exception {
        socketUDP.close();
    }

    /**
     * Handles client requests, handling the UDP packet received and perform concurrent modifications on the server state,
     * adding, removing and updating the Peers information.
     */
    class ListenerThread extends Thread {
        private DatagramPacket receivedPacket;

        public ListenerThread(DatagramPacket receivedPacket) {
            this.receivedPacket = receivedPacket;
        }

        @Override
        public void run() {
            Message clientMessage = readClientMessage();
            System.out.println("Mensagem recebida do sender= " + clientMessage + "\n");
            new MessageSenderThread(receivedPacket, MessageType.ACKNOWLEDGE).start();
        }

        private Message readClientMessage() {
            byte[] receivedData = this.receivedPacket.getData();

            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(receivedData);
                ObjectInputStream inputObject = new ObjectInputStream(new BufferedInputStream(byteArrayInputStream));
            ) {
                return (Message) inputObject.readObject();
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    class MessageSenderThread extends Thread {

        private final MessageType type;
        private final DatagramPacket datagramPacket;

        public MessageSenderThread(final DatagramPacket datagramPacket, MessageType type) {
            this.type = type;
            this.datagramPacket = datagramPacket;
        }

        @Override
        public void run() {
            System.out.println("Sending ACK to sender");
            Message message = new Message(type);
            Message.sendUdpMessage(message,datagramPacket.getAddress().getHostAddress(), datagramPacket.getPort(), socketUDP);
            System.out.println("Message successfully sent");
        }
    }

    private static String readServerAddress() {
        System.out.println("IP do Receiver:");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));){
            return reader.readLine();
        } catch (IOException e) {
            return "127.0.0.1";
        }
    }

    public static void main(String[] args) {
        String enderecoServidor = readServerAddress();

        try (Receiver Receiver = new Receiver(enderecoServidor)){
            Receiver.listenForMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
