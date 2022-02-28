package com.chatapp;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines a template for the message, with title and a body formed by a set of messages mapped in key-value fashion.
 *
 * @author Maik Henrique
 */
public class Message implements Serializable {

    private static final long serialVersionUID = -3969352858203924755L;

    private final Header header;
    private Map<String, Object> messages;

    public Message(MessageType messageType, Long messageIndex) {
        this.header = new Header(messageType, messageIndex);
        this.messages = new HashMap<>();
    }

    class Header implements Serializable {

        private final MessageType messageType;
        private final Long messageIndex;

        public Header(MessageType messageType, Long messageIndex) {
            this.messageType = messageType;
            this.messageIndex = messageIndex;
        }

        public MessageType getMessageType() {
            return messageType;
        }

        public Long getMessageIndex() {
            return messageIndex;
        }

        @Override
        public String toString() {
            return "Header [messageIndex=" + messageIndex + ", messageType=" + messageType + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getEnclosingInstance().hashCode();
            result = prime * result + ((messageIndex == null) ? 0 : messageIndex.hashCode());
            result = prime * result + ((messageType == null) ? 0 : messageType.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Header other = (Header) obj;
            if (messageIndex == null) {
                if (other.messageIndex != null)
                    return false;
            } else if (!messageIndex.equals(other.messageIndex))
                return false;
            if (messageType != other.messageType)
                return false;
            return true;
        }

        private Message getEnclosingInstance() {
            return Message.this;
        }

    }

    /**
     * Adds a body value to the message
     *
     * @param title title of the message
     * @param messageBody message body
     */
    public void addMessage(String title, Object messageBody) {
        messages.put(title, messageBody);
    }

    public Header getHeader() {
        return header;
    }

    public Map<String, Object> getMessages() {
        return messages;
    }

    /**
     * Sends UDP messages
     *
     * @param message message to be sent
     * @param address IP address of the destination
     * @param port port of the destination
     * @param socketUDP configured UDP socket
     */
    public static void sendUdpMessage(Message message, String address, int port, DatagramSocket socketUDP) {
        InetAddress destinationAddress;
        try {
            destinationAddress = InetAddress.getByName(address);
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new BufferedOutputStream(byteOutputStream));

            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            byte[] messageInBytes = byteOutputStream.toByteArray();

            DatagramPacket packet = new DatagramPacket(messageInBytes, messageInBytes.length, destinationAddress, port);
            socketUDP.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Receives UDP messages within size at most of
     *
     * @param socketUDP socket UDP utilitário para o recebimento de Message.
     * @return devolve uma instância construída da Message recebida.
     */
    public static Message receiveUdpMessage(DatagramSocket socketUDP) {
        byte[] receivedBytes = new byte[8 * 1024];
        DatagramPacket packet = new DatagramPacket(receivedBytes, receivedBytes.length);

        try {
            socketUDP.receive(packet);
        } catch (IOException e1) {
            return null;
        }

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(packet.getData());
            ObjectInputStream inputObject = new ObjectInputStream(new BufferedInputStream(byteArrayInputStream));) {
            return (Message) inputObject.readObject();
        } catch (ClassNotFoundException | IOException e) {
            return null;
        }
    }

    public static Message deserializarBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectStream = new ObjectInputStream(byteStream);
        Message message =  (Message) objectStream.readObject();
        objectStream.close();

        return message;
    }

    @Override
    public String toString() {
        return "Message [mensagens=" + messages + ", header=" + header + "]";
    }

    public static enum MessageType {
        PACKAGE,
        ACKNOWLEDGE
    }

    public static enum MessageBodyType {
        BODY("body"),
        INDEX("index");

        public final String label;

        private MessageBodyType(String label) {
            this.label = label;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((header == null) ? 0 : header.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Message other = (Message) obj;
        if (header == null) {
            if (other.header != null)
                return false;
        } else if (!header.equals(other.header))
            return false;
        return true;
    }
}
