package com.chatapp;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Console;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.List;
import java.util.Stack;
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
    private final DatagramSocket udpSocket;
    private final MessageListenerThread udpMessageListenerThread;
    public static final int WINDOW_LENGTH = 5;
    private long windowStartIndex;
    private ConcurrentHashMap<Long, BufferItem> pendingAcknowledgeBuffer;
    private long currentMessageIndex;
    private SendMessageStrategy sendMessageStrategy;
    private String RECEIVER_IP_ADDRESS = "127.0.0.1";
    private Timer timer;
    private final ConcurrentHashMap<Long, MessageTask> messageTasksMap;
    private static final long RESENDING_MESSAGE_PERIOD = 1000l;
    private final Stack<Message> outOfOrderMessages;

    /**
     * Implementação auxiliar para atingir o item 3.9 - Inicialização do sender
     *
     * Permite ao usuário configurar qual a porta destino do sender, além de inicializar a thread ouvinte de mensagens.
     *
     * @throws IOException
     */
    public Sender() throws IOException {
        this.windowStartIndex = 0;
        this.pendingAcknowledgeBuffer = new ConcurrentHashMap<>();
        currentMessageIndex = 0;

        this.keyboardReader = System.console();

        System.out.println(ConsoleMessageConstants.TARGET_RECEIVER_CONFIGURATION);

        System.out.println(ConsoleMessageConstants.ASK_RECEIVER_PORT);
        this.receiverPort = Integer.parseInt(keyboardReader.readLine());

        System.out.println(ConsoleMessageConstants.ASK_SENDER_PORT);
        this.listenerPort = Integer.parseInt(keyboardReader.readLine());

        this.udpSocket = new DatagramSocket(listenerPort);
        this.udpMessageListenerThread =  new MessageListenerThread();
        udpMessageListenerThread.start();

        this.timer = new Timer();
        this.messageTasksMap = new ConcurrentHashMap<>();

        this.outOfOrderMessages = new Stack<>();
    }


    private void setSendMessageStrategy(SendMessageStrategy strategy) {
        this.sendMessageStrategy = strategy;
    }


    @Override
    public void close() throws IOException {
        udpSocket.close();
        timer.cancel();
    }

    /**
     * Define em formato de constantes as mensagens que serão impressas no console, tem o intuito de padronizar e centralizar
     */
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

    /**
     * Define a interface base para as diferentes estratégias para envio de mensagem (lento, duplicado, etc.)
     * Assim, facilita a chamada e modulariza as diferentes estratégias.
     * Visa contemplar o item 3.6 do documento.
     */
    public interface SendMessageStrategy {
        /**
         * Método para envio de mensagens de acordo com a estratégia implementada.
         * @param message Mensagem que deverá ser encaminha ao Receiver
         */
        void send(Message message);
    }

    /**
     * Implementação do envio padrão de mensagem, sem nenhuma anomalia.
     * Atende o item 3.6 - Pacote enviado normalmente
     */
    class RegularMessageSenderStrategy implements SendMessageStrategy {

        /**
         * Inicializa normalmente a Thread MessageSenderThread, que é quem faz o envio de fato via UDP, também aciona o método
         * responsável por agendar o reenvio da mensagem de forma periódica, para o caso em que o Receiver responda com sucesso
         * com um ACK dentro da janela esperada.
         */
        @Override
        public void send(Message message) {
            new MessageSenderThread(message).start();
            createResendMessageTask(message);
        }
    }

    /**
     * Implementação da perda de mensagens enviadas.
     * Atende o item 3.6 - Pacote perdido.
     */
    class LostMessageSenderStrategy implements SendMessageStrategy {
        /**
         * A ideia é simplesmente não enviar o pacote e só agendar o reenvio periódico, como não é enviado, então obrigatoriamente
         * ocorrerá o "reenvio" via agendamento.
         */
        @Override
        public void send(Message message) {
            createResendMessageTask(message);
        }
    }

    /**
     * Implementação de envio de pacote lento.
     * Atende o item 3.6 - Pacotes lentos.
     */
    class SlowMessageSenderStrategy implements SendMessageStrategy{

        private final long delay;

        public SlowMessageSenderStrategy(long delay) {
            this.delay = delay;
        }

        /**
         * A estratégia de implementação é agendar o envio de mensagem com o auxílio da classe TimerTask, após a chamada do método
         * para a mensagem é enviada uma única vez após um período de tempo em milisegundos definido pelo atributo 'delay'.
         */
        @Override
        public void send(Message message) {
            TimerTask task = new DelayedMessageSenderTask(message);
            timer.schedule(task, delay);
        }

        /**
         * É executado com base no agendamente feito pelo Timer, simplesmente inicializa a Thread para o  envio de mensagens
         * e adiciona o agendamento periódico para reenvio em caso de atraso/perda do pacote.
         */
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
    }

    /**
     * Implementação de envio de pacote duplicado.
     * Atende o item 3.6 - Pacotes duplicados.
     */
    class DuplicatedMessageSenderStrategy implements SendMessageStrategy {

        /**
         * A estratégia é bem simples, simplesmente cria duas threads para o envio de mensagem e agenda o reenvio periódico.
         */
        @Override
        public void send(Message message) {
            new MessageSenderThread(message).start();
            new MessageSenderThread(message).start();
            createResendMessageTask(message);
        }
    }

    /**
    * Implementação de envio de pacote fora de ordem.
    * Atende o item 3.6 - Pacotes fora de ordem.
    * A estratégia é adicionar à uma Stack que contém as mensagens fora de ordem, assim quando qualquer outro tipo
    * de mensagem que não seja "fora de ordem" for executado, têm-se a remoção dos elementos da pilha e a chamada para envio como
    * mensagens do tipo lento, mas que terão delay de 200 ms. Na pilha teremos o armazenamento no formato mais-recente -> mais-antigo,
    * ainda adiciona-se um delay de 200 ms, para que evite-se de chegarem ao "mesmo tempo".
    */
    class OutOfOrderSenderStrategy implements SendMessageStrategy {

        /**
         * Nessa etapa, simplesmente adiciona a mensagem ao topo da pilha de mensagens fora de ordem e nem mesmo faz o agendamento para
         * reenvio.
         */
        @Override
        public void send(Message message) {
            outOfOrderMessages.push(message);
        }
    }

    /**
     * Representa um elemento do buffer e visa ajudar a contemplar o item 3.7 das funcionalidades.
     * A ideia é armazenar a mensagem e um booleano que decide se o ACK foi recebido ou não, de modo que só será removido do buffer
     * quando as lacunas forem preenchidas e a janela atualizada.
     */
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

    /**
     * Thread ouvinte de pacotes UDP que serão recebidas do Receiver.
     * Visa auxiliar no atingimento do item 4.1, para o recebimento de pacotes.
     *
     */
    class MessageListenerThread extends Thread {

        private DatagramPacket receivedPacket;

        @Override
        public void run() {
            while (true) {
                listenForMessages();
            }
        }

        /**
         * Quando recebe um pacote UDP, encaminha o tratamento.
         */
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

        /**
         * Faz a conversão do pacote recebido em bytes para o objeto Message e então faz a chamada para a atualização do buffer.
         */
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

    /**
     * Implementação visa auxiliar no alcance da funcionalidade 3.2 - Reenvio de pacotes por timeout.
     * Classe que é chamada para execução pelo Timer para o reenvio de pacotes.
     * A ideia é simplesmente criar e inicializar a thread para o envio de mensagem de forma direta.
     */
    class MessageTask extends TimerTask {
        private final Message message;

        public MessageTask(final Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            long messageIndex = message.getHeader().getMessageIndex();
            System.out.println(String.format(ConsoleMessageConstants.RESENDING_PACKAGE_MESSAGE, messageIndex));
            new MessageSenderThread(message).start();
        }
    }

    /**
     * Thread usada para envio de mensagens de forma paralela, assim sua execução se dá pelo uso do utilitário para envio de
     * mensagens da classe Message, com a passagem do Socket e demais parâmetros.
     */
    class MessageSenderThread extends Thread {

        private final Message message;

        public MessageSenderThread(final Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            Message.sendUdpMessage(this.message, RECEIVER_IP_ADDRESS, receiverPort, udpSocket);
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

    /**
     * Implementação utilizada para ajudar no alcance do item 3.7 - Implementação de buffer
     * Valida se a mensagem é do tipo ACK, caso seja e se encontre no buffer, chama o método achknowledge() que é usado
     * para o reconhecimento do recebimento adequado do ACK.
     *
     * O método é sincronizado para evitar acessos concorrentes ao buffer.
     *
     * @param receivedMessage Mensagem recebida do Receiver
     */
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

    /**
     * Implementação usada para auxiliar a completude do item 3.7 - Buffer e implementação do protocolo Selective Repeat
     * Usado para atualizar a janela do Sender, assim a ideia é pegar o elemento correspondente ao início da janela do buffer e se
     * estiver presente, irá incrementar o atributo windowStartIndex e removendo o elemento correspondente do buffer.
     */
    private synchronized void updateWindow() {
        BufferItem packageItem = pendingAcknowledgeBuffer.get(this.windowStartIndex);

        while (packageItem != null && !packageItem.isPendingAcknowledge) {
            pendingAcknowledgeBuffer.remove(this.windowStartIndex);
            this.windowStartIndex++;
            packageItem = pendingAcknowledgeBuffer.get(this.windowStartIndex);
        }
    }

    /**
     * A ideia desse método é cancelar os jobs de reenvio da última mensagem que foi recebida, pois já teve seu recebimento
     * confirmado pelo Receiver, tornando desnecessário o reenvio periódico.
     *
     * @param messageIndex Indíce do pacote que foi reconhecido pelo Receiver
     */
    private synchronized void updateTasks(long messageIndex) {
        MessageTask task = messageTasksMap.get(messageIndex);

        if (task == null) {
            return;
        }

        task.cancel();
        messageTasksMap.remove(messageIndex);
        timer.purge();
    }

    /**
     * Implementação auxiliar para atingir o item 3.7 - Implementação do buffer de pacotes e 3.1 - Cabeçalho das mensagens
     * Adiciona a mensagem que será enviada no buffer e incrementa o atributo currentMessageIndex, que representa o índice do pacote.
     *
     * @param message Mensagem recebida via input do usuário para envio ao Receiver
     */
    private void saveMessageOnBuffer(Message message) {
        BufferItem packageItem = new BufferItem(message);
        pendingAcknowledgeBuffer.put(this.currentMessageIndex, packageItem);
        this.currentMessageIndex++;
    }

    /**
     * Implementação auxiliar para atingir o item 3.7 - Reenvio de pacotes perdidos
     *
     * A ideia é usar um atributo de instância Timer que trabalha em uma thread separada com um relógio, este permite agendar tarefas
     * com instâncias do tipo TimerTask. A ideia é que as mensagens sejam reenviadas a cada 1 segundo.
     *
     * https://docs.oracle.com/javase/8/docs/api/java/util/Timer.html
     * https://docs.oracle.com/javase/8/docs/api/java/util/TimerTask.html
     *
     * @param message Mensagem que será agendada para reenvio periódico
     */
    private void createResendMessageTask(Message message) {
        long messageIndex = message.getHeader().getMessageIndex();

        if (messageTasksMap.contains(messageIndex)) {
            return;
        }

        MessageTask task = new MessageTask(message);
        timer.scheduleAtFixedRate(task, RESENDING_MESSAGE_PERIOD, RESENDING_MESSAGE_PERIOD);
        messageTasksMap.put(messageIndex, task);
    }

    /**
     * Implementação auxiliar para atingir o item 3.6 - Envio de pacotes fora de ordem
     *
     * A ideia é consumir os elementos da stack, que estão organizados do mais-recente->mais-antigo e assim
     * por meio da estratégia de envio lento de mensagens, fazer o envio com um delay de 200 para evitar casos de
     * que a mensagem seja enviada no mesmo instante.
     *
     * Conforme são enviadas as mensagens são removidas da Stack.
     *
     * @param userOptionIndex
     */
    private void sendOutOfOrderMessages(long userOptionIndex) {
        if (userOptionIndex == 2) {
            return;
        }

        setSendMessageStrategy(new SlowMessageSenderStrategy(TimeUnit.MILLISECONDS.toMillis(200)));

        while (!this.outOfOrderMessages.isEmpty()) {
            Message message = this.outOfOrderMessages.pop();
            this.sendMessageStrategy.send(message);

        }
    }

    /**
     * Implementação auxiliar para atingir o item 3.6 - Escolha do usuário pelo tipo de envio
     *
     * Método usado para redirecionar a escolha do usuário, assim define o atributo sendStrategy para uma instância
     * de acordo com a escolha do usuário.
     *
     * @param userOption - Input recebido do usuário sobre qual tipo de pacote deseja enviar.
     */
    private void updateMessageStrategy(int userOptionIndex) {
        SendMessageStrategy sendStrategy = null;

        if (userOptionIndex == 0)
            sendStrategy = new SlowMessageSenderStrategy(TimeUnit.SECONDS.toMillis(10));

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

    /**
     * Implementação auxiliar para atingir o item 3.6 - Escolha do usuário pelo tipo de envio e 3.7 - Gerenciamento de buffer e
     * 3.8 - Impressões relacionadas a mensagens
     *
     * Menu interativo para o envio de mensagens pelo usuário, assim caso o buffer esteja cheio, não permite
     * o envio de mensagens.
     *
     */
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
            IntStream.range(0, optionsList.size()).forEach(index -> System.out.printf("%d - %s", index, optionsList.get(index)));
            String userOption = keyboardReader.readLine();
            int userOptionIndex = Integer.valueOf(userOption);

            updateMessageStrategy(userOptionIndex);

            Message message = new Message(MessageType.PACKAGE, this.currentMessageIndex);
            message.addMessage(MessageBodyType.BODY.label, senderMessage);
            System.out.println(String.format(ConsoleMessageConstants.MESSAGE_SENT, senderMessage, optionsList.get(userOptionIndex), this.currentMessageIndex));
            saveMessageOnBuffer(message);

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
