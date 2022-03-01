package com.chatapp;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Console;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
    private SendMessageStrategy sendMessageStrategy;

    private Timer timer;
    private final ConcurrentHashMap<Long, MessageTask> messageTasksMap;
    private static final long RESENDING_MESSAGE_PERIOD = 1000l;
    private final Queue<Message> outOfOrderMessages;

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

        this.outOfOrderMessages = new LinkedList<>();
    }


    private void setSendMessageStrategy(SendMessageStrategy strategy) {
        this.sendMessageStrategy = strategy;
    }


    @Override
    public void close() throws IOException {
        udpSocket.close();
        timer.cancel();
    }

    static class ConsoleMessageConstants {
        public final static String TARGET_RECEIVER_CONFIGURATION = "Configurando receiver alvo";
        public final static String ASK_RECEIVER_IP = "Digite o endereço IP do receiver";
        public final static String ASK_RECEIVER_PORT = "Digite a porta do receiver";
        public final static String ASK_SENDER_PORT = "Digite a porta ouvinte";
        public final static String MENU_MESSAGE_REQUEST = "Digite a mensagem que deseja enviar";
        public final static String MENU_OPENING = "Digite o número do tipo de envio";
        public final static List<String> MENU_OPTIONS = List.of("lenta", "perda", "fora de ordem", "duplicada", "normal");
        public final static String MENU_SELECTION_ERROR = "Erro ao escolher opção, tente novamente";
        public final static String MESSAGE_SENT = "Mensagem \"%s\" enviada como [%s] com id %d";
        public final static String MESSAGE_RECEIVED = "Mensagem de id %d recebida pelo receiver";
        public final static String RESENDING_PACKAGE_MESSAGE = "Mensagem de id %d não teve o recebimento confirmado, e portanto será reenviada";
        public final static String BUFFER_FULL_MESSAGE = "O buffer de mensagem está cheio e enquanto não houver espaço disponível, novas mensagens serão rejeitadas";
    }

    public interface SendMessageStrategy {
        void send(Message message);
    }

    public class RegularMessageSenderStrategy implements SendMessageStrategy {

        @Override
        public void send(Message message) {
            new MessageSenderThread(message).start();
            createResendMessageTask(message);
        }
    }

    public class LostMessageSenderStrategy implements SendMessageStrategy {
        @Override
        public void send(Message message) {
            createResendMessageTask(message);
        }
    }

    public class SlowMessageSenderStrategy implements SendMessageStrategy{

        private final long delay;

        public SlowMessageSenderStrategy() {
            this.delay = TimeUnit.SECONDS.toMillis(10);
        }

        private final class DelayedMessageSenderTask extends TimerTask {
            Message message;

            public DelayedMessageSenderTask(Message message) {
                this.message = message;
            }

            @Override
            public void run() {
                new MessageSenderThread(message).start();
                createResendMessageTask(message);
            }
        }

        @Override
        public void send(Message message) {
            TimerTask task = new DelayedMessageSenderTask(message);
            timer.schedule(task, delay);
        }
    }

    public class DuplicatedMessageSenderStrategy implements SendMessageStrategy {

        @Override
        public void send(Message message) {
            new MessageSenderThread(message).start();
            new MessageSenderThread(message).start();
            createResendMessageTask(message);
        }
    }

    public class OutOfOrderSenderStrategy implements SendMessageStrategy {

        @Override
        public void send(Message message) {
            outOfOrderMessages.add(message);
        }
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

        if (messageTasksMap.contains(messageIndex)) {
            return;
        }

        MessageTask task = new MessageTask(message);
        timer.scheduleAtFixedRate(task, RESENDING_MESSAGE_PERIOD, RESENDING_MESSAGE_PERIOD);
        messageTasksMap.put(messageIndex, task);
    }

    private void sendOutOfOrderMessages(long userOptionIndex) {
        if (userOptionIndex == 2) {
            return;
        }

        setSendMessageStrategy(new RegularMessageSenderStrategy());
        outOfOrderMessages.forEach(message -> sendMessageStrategy.send(message));
    }

    /**
     * @TODO: Replace by factory
     * @param userOption
     */
    private void updateMessageStrategy(int userOptionIndex) {
        SendMessageStrategy sendStrategy = null;

        if (userOptionIndex == 0)
            sendStrategy = new SlowMessageSenderStrategy();

        if (userOptionIndex == 1)
            sendStrategy = new LostMessageSenderStrategy();

        if (userOptionIndex == 2)
            sendStrategy = new OutOfOrderSenderStrategy();

        if (userOptionIndex == 3)
            sendStrategy = new DuplicatedMessageSenderStrategy();

        if (userOptionIndex == 4)
            sendStrategy = new RegularMessageSenderStrategy();

        setSendMessageStrategy(sendStrategy);
    }

    public void interactiveMenu() {
        while (true) {
            boolean isBufferFull = pendingAcknowledgeBuffer.size() == WINDOW_LENGTH;

            if (isBufferFull) {
                System.out.println(ConsoleMessageConstants.BUFFER_FULL_MESSAGE);
                continue;
            }

            List<String> optionsList = ConsoleMessageConstants.MENU_OPTIONS;

            System.out.println(ConsoleMessageConstants.MENU_MESSAGE_REQUEST);
            String senderMessage = keyboardReader.readLine();

            System.out.println(ConsoleMessageConstants.MENU_OPENING);
            IntStream.range(0, optionsList.size()).forEach(index -> System.out.println(index + " - " + optionsList.get(index)));
            String userOption = keyboardReader.readLine();
            int userOptionIndex = Integer.valueOf(userOption);

            updateMessageStrategy(userOptionIndex);

            Message message = new Message(MessageType.PACKAGE, this.currentMessageIndex);
            message.addMessage(MessageBodyType.BODY.label, senderMessage);
            saveMessageOnBuffer(message);

            System.out.println(String.format(ConsoleMessageConstants.MESSAGE_SENT, senderMessage, optionsList.get(userOptionIndex), this.windowStartIndex));

            sendMessageStrategy.send(message);

            sendOutOfOrderMessages(userOptionIndex);
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
