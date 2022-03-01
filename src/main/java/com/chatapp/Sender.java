package com.chatapp;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Console;
import java.io.IOError;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.chatapp.Message.Header;
import com.chatapp.Message.MessageBodyType;
import com.chatapp.Message.MessageType;

public final class Sender implements AutoCloseable{

    /**
     *  Sets the default package size for transfer, which is by default 8 Kb.
     */
    public static final int PACKET_TRANSFER_SIZE = 1024 * 8;
    private final Console keyboardReader;
    private final int receiverPort;
    private final int listenerPort;
    private final String receiverIpAddress;
    private final DatagramSocket udpSocket;
    private final MessageListenerThread udpMessageListenerThread;
    public static final int WINDOW_LENGTH = 5;
    private long windowStartIndex;
    private ConcurrentHashMap<Long, BufferItem> pendingAcknowledgeBuffer;
    private long currentMessageIndex;

    private Timer timer;
    private final ConcurrentHashMap<Long, MessageTask> messageTasksMap;
    private static final long RESENDING_MESSAGE_PERIOD = 1000l;

    public Sender() throws IOException {
        this.windowStartIndex = 0;
        this.pendingAcknowledgeBuffer = new ConcurrentHashMap<>();
        currentMessageIndex = 0;

        this.keyboardReader = System.console();

        System.out.println(ConsoleMessageConstants.TARGET_RECEIVER_CONFIGURATION);
        System.out.println(ConsoleMessageConstants.ASK_RECEIVER_IP);
        this.receiverIpAddress = keyboardReader.readLine();

        System.out.println(ConsoleMessageConstants.ASK_RECEIVER_PORT);
        this.receiverPort = Integer.parseInt(keyboardReader.readLine());

        System.out.println(ConsoleMessageConstants.ASK_SENDER_PORT);
        this.listenerPort = Integer.parseInt(keyboardReader.readLine());

        this.udpSocket = new DatagramSocket(listenerPort);
        this.udpMessageListenerThread =  new MessageListenerThread();
        udpMessageListenerThread.start();

        this.timer = new Timer();
        this.messageTasksMap = new ConcurrentHashMap<>();
    }

    static class ConsoleMessageConstants {
        public final static String TARGET_RECEIVER_CONFIGURATION = "Configurando receiver alvo";
        public final static String ASK_RECEIVER_IP = "Digite o endereço IP do receiver";
        public final static String ASK_RECEIVER_PORT = "Digite a porta do receiver";
        public final static String ASK_SENDER_PORT = "Digite a porta ouvinte";
        public final static String MENU_MESSAGE_REQUEST = "Digite a mensagem que deseja enviar";
        public final static String MENU_OPENING = "Escolha uma das opções de tipo de envio";
        public final static String[] MENU_OPTIONS = {"lenta", "perda", "fora de ordem", "duplicada", "normal"};
        public final static String MENU_SELECTION_ERROR = "Erro ao escolher opção, tente novamente";
        public final static String MESSAGE_SENT = "Mensagem \"%s\" enviada como [%s] com id %d";
        public final static String MESSAGE_RECEIVED = "Mensagem de id %d recebida pelo receiver";
        public final static String RESENDING_PACKAGE_MESSAGE = "Mensagem de id %d não teve o recebimento confirmado, e portanto será reenviada";
        public final static String BUFFER_FULL_MESSAGE = "O buffer de mensagem está cheio e enquanto não houver espaço disponível, novas mensagens serão rejeitadas";
    }

    @Override
    public void close() throws IOException {
        udpSocket.close();
        timer.cancel();
    }

    class BufferItem {
        private final Message message;
        private boolean isPendingAcknowledge;

        public BufferItem(final Message message) {
            this.message = message;
            this.isPendingAcknowledge = true;
        }

        public Message getMessage() {
            return message;
        }

        public boolean isPendingAcknowledge() {
            return isPendingAcknowledge;
        }

        public void acknowledge() {
            this.isPendingAcknowledge = false;
        }
    }

    class MessageListenerThread extends Thread {

        private DatagramPacket receivedPacket;

        @Override
        public void run() {
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


        private void handleReceivedMessage() {
            byte[] receivedData = this.receivedPacket.getData();

            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(receivedData);
                ObjectInputStream inputObject = new ObjectInputStream(new BufferedInputStream(byteArrayInputStream))) {

                Message message = (Message) inputObject.readObject();
                updateBuffer(message);
            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }
    }

    class MessageTask extends TimerTask {
        private final Message message;

        public MessageTask(final Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            new MessageSenderThread(message).start();
        }
    }

    class MessageSenderThread extends Thread {

        private final Message message;

        public MessageSenderThread(final Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            Message.sendUdpMessage(this.message, receiverIpAddress, receiverPort, udpSocket);
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

    private synchronized void updateBuffer(Message receivedMessage) {
        Header header = receivedMessage.getHeader();

        if (!MessageType.ACKNOWLEDGE.equals(header.getMessageType())) {
            return;
        }

        long index = header.getMessageIndex();

        pendingAcknowledgeBuffer.computeIfPresent(index, (key, value) -> {
            value.acknowledge();
            return value;
        });

        System.out.println(String.format(ConsoleMessageConstants.MESSAGE_RECEIVED, index));

        updateTasks(index);
        updateWindow();
    }

    private synchronized void updateWindow() {
        BufferItem packageItem = pendingAcknowledgeBuffer.get(this.windowStartIndex);

        while (packageItem != null && !packageItem.isPendingAcknowledge) {
            pendingAcknowledgeBuffer.remove(this.windowStartIndex);
            this.windowStartIndex++;
            packageItem = pendingAcknowledgeBuffer.get(this.windowStartIndex);
        }
    }

    private synchronized void updateTasks(long messageIndex) {
        MessageTask task = messageTasksMap.get(messageIndex);

        if (task == null) {
            return;
        }

        task.cancel();
        messageTasksMap.remove(messageIndex);
        timer.purge();
    }

    private void saveMessageOnBuffer(Message message) {
        BufferItem packageItem = new BufferItem(message);
        pendingAcknowledgeBuffer.put(this.currentMessageIndex, packageItem);
        this.currentMessageIndex++;
    }

    private void createResendMessageTask(Message message) {
        long messageIndex = message.getHeader().getMessageIndex();
        MessageTask task = new MessageTask(message);
        timer.scheduleAtFixedRate(task, RESENDING_MESSAGE_PERIOD, RESENDING_MESSAGE_PERIOD);
        messageTasksMap.put(messageIndex, task);
    }

    public void interactiveMenu() {
        while (true) {
            try {
                boolean isBufferFull = pendingAcknowledgeBuffer.size() == WINDOW_LENGTH;
                if (isBufferFull) {
                    System.out.println(ConsoleMessageConstants.BUFFER_FULL_MESSAGE);
                    continue;
                }

                String senderMessager = getSenderMessage();

                Message message = new Message(MessageType.PACKAGE, this.currentMessageIndex);
                message.addMessage(MessageBodyType.BODY.label, senderMessager);

                saveMessageOnBuffer(message);
                createResendMessageTask(message);

                MessageSenderThread senderThread = new MessageSenderThread(message);
                senderThread.start();

            } catch (IOError e) {
                System.err.println(ConsoleMessageConstants.MENU_SELECTION_ERROR);
            }
        }
    }

    private String getSenderMessage() {
        System.out.println(ConsoleMessageConstants.MENU_MESSAGE_REQUEST);
        String senderMessager = keyboardReader.readLine();
        return senderMessager;
    }

    public static void main(String[] args) {
        try (Sender Sender = new Sender();) {
            Sender.interactiveMenu();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

}
