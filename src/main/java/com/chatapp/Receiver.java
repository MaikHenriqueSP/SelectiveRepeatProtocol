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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;

import com.chatapp.Message.Header;
import com.chatapp.Message.MessageBodyType;
import com.chatapp.Message.MessageType;

public class Receiver implements AutoCloseable {

    private final DatagramSocket socketUDP;
    public static final int SOCKET_RECEIVED_PORT = 10098;
    public static final String SERVER_ADDRESS = "localhost";
    public final String FICTITIOUS_SERVER_ADDRESS;
    private static final int TRANSFER_PACKET_SIZE = 8 * 1024;
    private static final String STANDARD_IP_ADDRESS = "127.0.0.1";
    private long windowStartIndex;
    private final Queue<Message> messageBuffer;


    public Receiver(String serverAddress) throws IOException {
        this.windowStartIndex = 0;
        this.socketUDP = new DatagramSocket(SOCKET_RECEIVED_PORT);
        this.FICTITIOUS_SERVER_ADDRESS = serverAddress;
        this.messageBuffer = new PriorityQueue<>((a, b) -> a.getHeader().getMessageIndex().compareTo(b.getHeader().getMessageIndex()));
    }

    static class ConsoleMessageConstants {
        public final static String DUPLICATED_MESSAGE = "Mensagem de id %d recebida de forma duplicada";
        public final static String UNORDERED_MESSAGE = "Mensagem de id %d recebida fora de ordem, ainda não recebidos os identificadores [%s]";
        public final static String ORDERED_MESSAGE = "Mensagem de id %d recebida na ordem, entregando para a camada de aplicação";
        public final static String ERROR_TO_RECEIVE_MESSAGE = "Ocorreu um erro ao receber a mensagem";
        public final static String INVALID_MESSAGE = "Mensagem recebida é inválida!";
        public final static String BUFFER_FULL_ERROR = "Buffer cheio, rejeitando a mensagem";
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
            Optional<Message> optionalMessage = readClientMessage();

            if (optionalMessage.isEmpty()) {
                System.out.println(ConsoleMessageConstants.ERROR_TO_RECEIVE_MESSAGE);
                return;
            }

            Message senderMessage = optionalMessage.get();

            if (!isValidMessage(senderMessage)) {
                System.out.println(ConsoleMessageConstants.INVALID_MESSAGE);
                return;
            }

            boolean isMessageSuccessfullyHandled = handleReceivedMessage(senderMessage);

            if (isMessageSuccessfullyHandled) {
                new MessageSenderThread(receivedPacket, senderMessage.getHeader()).start();
            }

        }

        private Optional<Message> readClientMessage() {
            byte[] receivedData = this.receivedPacket.getData();

            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(receivedData);
                ObjectInputStream inputObject = new ObjectInputStream(new BufferedInputStream(byteArrayInputStream));
            ) {
                Message receivedMessage = (Message) inputObject.readObject();
                return Optional.of(receivedMessage);
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }

            return Optional.empty();
        }
    }

    private boolean isValidMessage(Message message) {
        Header header = message.getHeader();

        if (header == null || !MessageType.PACKAGE.equals(header.getMessageType()) || header.getMessageIndex() == null) {
            return false;
        }

        Map<String, Object> messages = message.getMessages();

        if (messages == null || !messages.containsKey(MessageBodyType.BODY.label)) {
            return false;
        }

        return true;
    }

    private synchronized boolean handleReceivedMessage(Message message) {
        long messageIndex = message.getHeader().getMessageIndex();

        boolean isDuplicatedMessage = messageIndex < this.windowStartIndex || messageBuffer.contains(message);
        if (isDuplicatedMessage) {
            System.out.println(String.format(ConsoleMessageConstants.DUPLICATED_MESSAGE, messageIndex));
            return true;
        }

        if (isBufferFull() && messageIndex != this.windowStartIndex) {
            System.out.println(ConsoleMessageConstants.BUFFER_FULL_ERROR);
            return false;
        }

        boolean isAdded = messageBuffer.offer(message);

        if (!isAdded) {
            return false;
        }

        boolean isCurrentExpectedMessage = messageBuffer.peek().getHeader().getMessageIndex().equals(this.windowStartIndex);

        if (messageBuffer.peek().equals(message) && isCurrentExpectedMessage) {
            System.out.println(String.format(ConsoleMessageConstants.ORDERED_MESSAGE, messageIndex));
        } else {
            String missingMessages = getMissingIndexes(messageIndex);
            System.out.println(String.format(ConsoleMessageConstants.UNORDERED_MESSAGE, messageIndex, missingMessages));
        }

        while (!messageBuffer.isEmpty() && messageBuffer.peek().getHeader().getMessageIndex().equals(this.windowStartIndex)) {
            messageBuffer.poll();
            this.windowStartIndex++;
        }

        return true;
    }

    private boolean isBufferFull() {
        return Sender.WINDOW_LENGTH == messageBuffer.size();
    }

    private String getMissingIndexes(long lastReceivedIndex) {
        StringBuilder builder = new StringBuilder();

        Map<Long, Message> alreadyReceivedMessages = new HashMap<>();

        for (Message message : messageBuffer) {
            long messageIndex = message.getHeader().getMessageIndex();
            if (messageIndex >= this.windowStartIndex && messageIndex < lastReceivedIndex) {
                alreadyReceivedMessages.put(messageIndex, message);
            }
        }

        for (long i = this.windowStartIndex; i < lastReceivedIndex; i++) {
            if (!alreadyReceivedMessages.containsKey(i)) {
                builder.append(" " + i + " ");
            }
        }

        return builder.toString();
    }

    class MessageSenderThread extends Thread {

        private final Header header;
        private final DatagramPacket datagramPacket;

        public MessageSenderThread(final DatagramPacket datagramPacket, Header header) {
            this.header = header;
            this.datagramPacket = datagramPacket;
        }

        @Override
        public void run() {
            Message message = new Message(MessageType.ACKNOWLEDGE, header.getMessageIndex());
            Message.sendUdpMessage(message,datagramPacket.getAddress().getHostAddress(), datagramPacket.getPort(), socketUDP);
        }
    }

    private static String readServerAddress() {
        System.out.println("IP do Receiver:");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));){
            return reader.readLine();
        } catch (IOException e) {
            return STANDARD_IP_ADDRESS;
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
