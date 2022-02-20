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

    private final String title;
    private final Map<String, Object> messages;

    public Message(String title) {
        this.title = title;
        this.messages = new HashMap<>();
    }

    /**
     * Adds a body value to the message
     *
     * @param title title of the message
     * @param messageBody message body
     */
    public void adicionarMensagem(String title, Object messageBody) {
        messages.put(title, messageBody);
    }

    public String getTitulo() {
        return title;
    }

    public Map<String, Object> getMensagens() {
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
    public static void enviarMensagemUDP(Message message, String address, int port, DatagramSocket socketUDP) {
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
    public static Message receberMensagemUDP(DatagramSocket socketUDP) {
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
        return "Message [mensagens=" + messages + ", titulo=" + title + "]";
    }

}
