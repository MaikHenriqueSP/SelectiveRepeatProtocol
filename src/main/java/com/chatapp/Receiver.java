package com.chatapp;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Scanner;

import com.chatapp.Message.Header;
import com.chatapp.Message.MessageBodyType;
import com.chatapp.Message.MessageType;

public class Receiver implements AutoCloseable {

    private final DatagramSocket socketUDP;
    public static final int SOCKET_RECEIVED_PORT = 10098;
    private static final int TRANSFER_PACKET_SIZE = 8 * 1024;
    private long windowStartIndex;
    /**
     * Buffer utilizado para atender item 3.7.
     *
     * A estrutura de dados escolhida é o PriorityQueue, de forma que ordena-se de forma crescentem as mensagens pelo seu
     * índice, assim sempre teremos ao topo a mensagem mais antiga.
     */
    private final Queue<Message> messageBuffer;


    /**
     * Usado para a configuração do Receiver, requisita somente a porta ouvinte e define automaticamente o IP como localhost
     *
     * @param porta
     * @throws IOException
     */
    public Receiver(int porta) throws IOException {
        this.windowStartIndex = 0;
        this.socketUDP = new DatagramSocket(porta);
        this.messageBuffer = new PriorityQueue<>((a, b) -> a.getHeader().getMessageIndex().compareTo(b.getHeader().getMessageIndex()));
    }

    /**
     * Define em formato de constantes as mensagens que serão impressas no console, tem o intuito de padronizar e centralizar
     */
    static class ConsoleMessageConstants {
        public final static String DUPLICATED_MESSAGE = "Mensagem de id %d recebida de forma duplicada";
        public final static String UNORDERED_MESSAGE = "Mensagem de id %d recebida fora de ordem, ainda não recebidos os identificadores [%s]";
        public final static String ORDERED_MESSAGE = "Mensagem de id %d recebida na ordem, entregando para a camada de aplicação";
        public final static String ERROR_TO_RECEIVE_MESSAGE = "Ocorreu um erro ao receber a mensagem";
        public final static String INVALID_MESSAGE = "Mensagem recebida é inválida!";
        public final static String BUFFER_FULL_ERROR = "Buffer cheio, rejeitando a mensagem";
    }

    /**
     * Ouvinte do receveir, ou seja, fica esperando por mensagens UDP na thread principal e quando uma mensagem é recebida
     * cria uma nova thread com o ListenerThread para que o tratamento seja em paralelo e não bloqueie o recebimento de novas
     * mensagens
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void listenForMessages() throws IOException, ClassNotFoundException {
        while (true) {
            byte[] receivedBytes = new byte[TRANSFER_PACKET_SIZE];
            DatagramPacket packet = new DatagramPacket(receivedBytes, receivedBytes.length);
            socketUDP.receive(packet);
            new MessageHandlerThread(packet).start();
        }
    }

    @Override
    public void close() throws Exception {
        socketUDP.close();
    }

    /**
     * Faz o tratamento de mensagens UDP recebidas do Sender e faz o redirecionamento da resposta ACK caso o pacote recebido
     * seja válido.
     */
    class MessageHandlerThread extends Thread {
        private DatagramPacket receivedPacket;

        public MessageHandlerThread(DatagramPacket receivedPacket) {
            this.receivedPacket = receivedPacket;
        }

        /**
         * A ideia é validar a mensagem e iniciar uma nova thread para fazer o envio do ACK.
         * A partir do sucesso ou não do tratamento da mensagem no método handleReceivedMessage, faz-se ou não
         * o envio da mensagem de ACK de volta para o Sender.
         */
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

        /**
         * Converte o array de bytes recebidos em uma instância de Message
         * @return Uma instância de mensagem ou um optional vazio caso a mensagem lida seja inválida ou corrompida.
         */
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

    /**
     * Valida mensagens, simplesmente checa se tem mensagem no corpo e se o tipo da mensagem é PACKAGE
     * @param message Mensagem recebida do Sender
     * @return true se a mensagem é válida
     */
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

    /**
     * Implementação auxiliar para atingir os itens de 3.2 ao 3.5 - Tratamento dos diferentes tipos de envio de mensagem
     *
     * Caso o buffer esteja cheio e a mensagem não seja a esperada para completar a lacuna de início da janela, então a mensagem
     * é considerada inválida e o método retorna false e logo não é enviado um ACK para o Sender.
     *
     * 3.3 - Fora de ordem
     *  Todo pacote recebido que é válido, é adicionado ao buffer, como o buffer é ordenado pelo índice dos pacotes, temos
     *  para a impressão de mensagem de reconhecimento que se o elemento que está no topo do buffer não tem índice igual ao valor do ponteiro
     *  início da janela (windowStartIndex), este é considerado fora de ordem e então o ponteiro não é atualizado e nenhum item do buffer é removido,
     *  mas como a implementação segue o princípio de enviar ACKs indivíduais do SR, então um ACK para este pacote é enviado.
     *
     *  Caso seja o pacote que completa a primeira lacuna da janela, ou seja, tem índice igual a windowStartIndex, o valor de windowStartIndex
     *  é incrementado e o correspondente elemento do topo do buffer é removido e isso se repete enquanto o valor do índice do
     *  pacote for igual à windowStartIndex.
     *
     * 3.4 - Mensagem duplicada
     *  A verificação de mensagens duplicadas é feita segundo duas bases, o buffer e o ponteiro de início da janela, assim
     *  caso a mensagem recebida esteja no buffer, ou seu índice seja menor que índice de início da janela, a mensagem é
     *  então considerada como duplicada e então true é retornado, permitindo a resposta de ACK.
     *
     * 3.5 - Pacotes lentos
     *  Não tem impacto no Receiver a não ser que outra mensagem seja recebida antes, de modo que nesses casos o tratamento é o
     *  mesmo que para pacotes fora de ordem.
     *
     * @param message
     * @return
     */
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

        updateWindow();

        return true;
    }

    /**
     * Implementação auxiliar para atingir os itens de 3.7 - Buffer e janela do SR
     * A ideia da implementação é avançar o ponteiro da janela e remover elementos do buffer enquanto o elemento
     * do topo tiver índice igual a windowStartIndex
     */
    private void updateWindow() {
        while (!messageBuffer.isEmpty() && messageBuffer.peek().getHeader().getMessageIndex().equals(this.windowStartIndex)) {
            messageBuffer.poll();
            this.windowStartIndex++;
        }
    }

    /**
     * Implementação auxiliar para atingir o item de 3.7 - Buffer
     * O buffer é considerado cheio se o tamanho da janela é igual ao número de elementos no buffer
     * @return true se o buffer está cheio
     */
    private boolean isBufferFull() {
        return Sender.WINDOW_LENGTH == messageBuffer.size();
    }

    /**
     * Implementação auxiliar para atingir o item de 4.2 - Impressão dos pacotes faltantes
     *
     * @param lastReceivedIndex - Índice do último pacote recebido
     * @return Elementos que faltam para completar lacuna
     */
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

    /**
     * Thread usada para enviar mensagens ao Sender
     */
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
            Message.sendUdpMessage(message, datagramPacket.getAddress().getHostAddress(), datagramPacket.getPort(), socketUDP);
        }
    }

    private static int readServerPort() {
        System.out.println("Digite a porta do Receiver:");
        try (Scanner scanner = new Scanner(System.in)){
            return scanner.nextInt();
        } catch (Exception e) {
            return SOCKET_RECEIVED_PORT;
        }
    }

    public static void main(String[] args) {
        int porta = readServerPort();

        try (Receiver Receiver = new Receiver(porta)){
            Receiver.listenForMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
