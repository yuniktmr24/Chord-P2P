package csx55.chord;

import csx55.domain.ClientConnection;
import csx55.domain.Node;
import csx55.domain.RequestType;
import csx55.domain.UserCommands;
import csx55.transport.TCPConnection;
import csx55.transport.TCPServerThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.logging.Logger;

public class Peer extends Node implements Serializable {
    private static final long serialversionUID = 1L;
    private static final Logger logger = Logger.getLogger(Peer.class.getName());

    private FingerTable fingerTable;

    private List<String> fileNames;

    private String nodeIp;

    private int nodePort;

    private Integer peerId = null;

    private List <Peer> neighbors = new ArrayList<>();

    private List <String> storedFilePaths = new ArrayList<>();

    private transient TCPConnection discoveryConnection;

    private void setServiceDiscovery (String nodeIp, int nodePort) {
        setNodeIp(nodeIp);
        setNodePort(nodePort);
        setPeerId();
    }

    public static void main (String [] args) {
        //try (Socket socketToRegistry = new Socket(args[0], Integer.parseInt(args[1]));
        try (Socket socketToRegistry = new Socket("localhost", 12341);
             ServerSocket peerServer = new ServerSocket(0);
        ) {
            System.out.println("Connecting to server...");

            Peer peer = new Peer();
            peer.setServiceDiscovery(InetAddress.getLocalHost().getHostAddress(), peerServer.getLocalPort());

            Thread messageNodeServerThread = new Thread(new TCPServerThread(peer, peerServer));
            messageNodeServerThread.start();

            peer.joinChord(peer, socketToRegistry);

            Thread userThread = new Thread(() -> peer.userInput(peer));
            userThread.start();

            while (true) {

            }
        } catch (IOException e) {
            logger.severe("Error in main thread" + e.getMessage());
        }
    }

    //send registry message to discovery
    public void joinChord (Peer peer, Socket socketToRegistry) {
        try {
            ClientConnection joinChord = new ClientConnection(RequestType.JOIN_CHORD, this);

            TCPConnection connection = new TCPConnection(peer, socketToRegistry);

            connection.getSenderThread().sendObject(joinChord);
            connection.startConnection();
            //connection.getSenderThread().sendData();
            this.discoveryConnection = connection;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    //deregister
    public void leaveChord (Peer peer) {
        ClientConnection leaveChord = new ClientConnection(RequestType.LEAVE_CHORD, this);
        //this.discoveryConnection.getSenderThread().sendData();
    }

    public void printNeighbors() {}

    public void upload(String filePath) {
        //read file. send to node responsible fore it
        sendToNodeResponsibleForFile();
    }

    private void sendToNodeResponsibleForFile() {
    }

    public void download(String fileToDownloadWithExtension) {}

    public FingerTable getFingerTable() {
        return fingerTable;
    }

    public void setFingerTable(FingerTable fingerTable) {
        this.fingerTable = fingerTable;
    }

    public List<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(List<String> fileNames) {
        this.fileNames = fileNames;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    private void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public int getNodePort() {
        return nodePort;
    }



    private void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    public String getPeerDescriptor () {
        return this.nodeIp + ":" + this.nodePort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Peer peer = (Peer) o;
        return Objects.equals(nodeIp + ":" + nodePort, peer.nodeIp + ":" + peer.nodePort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeIp + ":" + nodePort);
    }

    public int getPeerId() {
        this.peerId = peerId == null ? hashCode() : this.peerId;
        return this.peerId;
    }

    private void userInput (Peer node) {
        try {
            boolean running = true;
            while (running) {
                // Scanner scan = new Scanner(System.in);
                System.out.println("***************************************");
                System.out.println("[Messaging Node] Enter your Message Node command");
                System.out.println(UserCommands.messageNodeCommandsToString());
                //String userInput = scan.nextLine();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                String userInput = inputReader.readLine();
                System.out.println("User input detected " + userInput);
                boolean containsSpace = false, validUploadFilesCmd = false, validDownloadFilesCmd = false;
                String uploadFilePath = ""; //local path to file to be uploaded
                String downloadFileName = "";
                if (userInput.contains(" ")) {
                    containsSpace = true;
                    if (userInput.startsWith(UserCommands.UPLOAD_FILE.getCmd()) ||
                            userInput.toUpperCase().contains("upload") ||
                            userInput.startsWith(String.valueOf(UserCommands.UPLOAD_FILE.getCmdId()))) {
                        validUploadFilesCmd = true;
                        uploadFilePath = userInput.split(" ")[1];
                    }
                    else if (userInput.startsWith(UserCommands.DOWNLOAD_FILE.getCmd()) ||
                            userInput.toUpperCase().contains("download") ||
                            userInput.startsWith(String.valueOf(UserCommands.DOWNLOAD_FILE.getCmdId()))) {
                        validDownloadFilesCmd = true;
                        downloadFileName = userInput.split(" ")[1];
                    }
                }
                if (userInput.equals(UserCommands.EXIT.getCmd()) || userInput.equals(String.valueOf(UserCommands.EXIT.getCmdId()))) {
                    //exit everything
                    running = false;
                    System.out.println("[Messaging Node] Exiting Overlay...");
                    ClientConnection conn2 = new ClientConnection(RequestType.LEAVE_CHORD, node);
//                    byte[] dataToSend2 = conn2.marshal();
//                    this.registryConnection.getSenderThread().sendData(dataToSend2);
                    //  TimeUnit.SECONDS.sleep(3);
                    // this.registryConnection.closeConnection();
                } else if (userInput.equals(UserCommands.PRINT_NEIGHBORS.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_NEIGHBORS.getCmdId()))) {
                    node.getNeighbors().forEach((k) -> {
                        System.out.println(k.getPeerDescriptor());
                    });
                }
                else if (userInput.equals(UserCommands.PRINT_FILES.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_FILES.getCmdId()))) {
                    node.getStoredFilePaths().forEach(System.out::println);
                }
                //for testing
                else if (userInput.equals(UserCommands.FINGER_TABLE.getCmd()) || userInput.equals(String.valueOf(UserCommands.FINGER_TABLE.getCmdId()))) {
                    System.out.println(node.getFingerTable().toString());
                }
                //for testing
                else if (containsSpace && validUploadFilesCmd) {
                    node.upload(uploadFilePath);
                }
                else if (containsSpace && validDownloadFilesCmd) {
                    node.download(downloadFileName);
                }
            }

        } catch (IOException ex) {

        }
    }

    public List<Peer> getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(List<Peer> neighbors) {
        this.neighbors = neighbors;
    }

    public void setPeerId() {
        this.peerId = hashCode();
    }

    public List<String> getStoredFilePaths() {
        return storedFilePaths;
    }

    public void appendToStoredFilePaths(String storedFilePaths) {
        this.storedFilePaths.add(storedFilePaths);
    }
}
