package csx55.chord;

import csx55.config.ChordConfig;
import csx55.domain.*;
import csx55.transport.TCPConnection;
import csx55.transport.TCPServerThread;
import csx55.util.FileUtils;
import csx55.util.FileWrapper;

import javax.swing.*;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Peer extends Node implements Serializable {
    private transient final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private static final long serialversionUID = 1L;
    private static final Logger logger = Logger.getLogger(Peer.class.getName());

    private FingerTable fingerTable;

    private transient FingerTable boostrapNodeFingerTable;

    private List<String> fileNames;

    private String nodeIp;

    private int nodePort;

    private Integer peerId = null;

    private String predecessorNode;

    private String successorNode;

    private Map <String, TCPConnection> tcpCache = new HashMap<>();

    private String fileStorageDirectory;

    private List <String> storedFilePaths = new ArrayList<>();

    private transient TCPConnection discoveryConnection;

    private transient CountDownLatch fingerTableRequestLatch;

    private void setServiceDiscovery (String nodeIp, int nodePort) {
        setNodeIp(nodeIp);
        setNodePort(nodePort);
        setAndGetPeerId();
        setAndGetStorageDirectory();
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

            peer.runMaintenance();
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


    public void upload(String filePath) {
        // OR: Processing lines as a Stream for more efficient memory usage
        System.out.println("Reading file from path "+ filePath);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            //stream.forEach(System.out::println);
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            FileWrapper file = new FileWrapper(fileBytes, fileName);
            //send to node responsible fore it
            sendToNodeResponsibleForFile(file);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void sendToNodeResponsibleForFile(FileWrapper file) {
        int dataKey = file.hashCode();
        FingerTableEntry possibleSuccessorEntry = this.fingerTable.lookup(dataKey);

        String possibleSuccessor = possibleSuccessorEntry.getSuccessorNode();
        if (possibleSuccessor.equals(this.getPeerDescriptor())) {
            //store at self
            storeIncomingFileBytes(file);

        }
        //got to send it to some other node
        //request FT from other node and proceed from there
        else {
            if (tcpCache.containsKey(possibleSuccessor)) {
                Message msg = new Message(Protocol.REQUEST_FINGER_TABLE, "");
                try {
                    fingerTableRequestLatch = new CountDownLatch(1);
                    tcpCache.get(possibleSuccessor).getSenderThread().sendData(msg);
                    fingerTableRequestLatch.await();
                    System.out.println("Received FT from intermediary node");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void download(String fileToDownloadWithExtension) {}

    public FingerTable getFingerTable() {
        return fingerTable;
    }

    public void initFingerTable() {
        this.fingerTable = new FingerTable();
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


    public int getPeerId() {
        this.peerId = peerId == null ? setAndGetPeerId() : this.peerId;
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
                    System.out.println(node.getNeighbors());
                }
                else if (userInput.equals(UserCommands.PRINT_FILES.getCmd()) || userInput.equals(String.valueOf(UserCommands.PRINT_FILES.getCmdId()))) {
                    node.getStoredFilePaths().forEach(System.out::println);
                }
                //for testing
                else if (userInput.equals(UserCommands.FINGER_TABLE.getCmd()) || userInput.equals(String.valueOf(UserCommands.FINGER_TABLE.getCmdId()))) {
                    System.out.println("Current Node ID : "+ this.getPeerId());
                    node.getFingerTable().printFingerTable();
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

    public String getNeighbors() {
        StringBuilder sb = new StringBuilder();

        sb.append("Predecessor "+ predecessorNode);
        sb.append(" | Successor" + successorNode);

        return sb.toString();
    }


    public int setAndGetPeerId() {
        String toHash = nodeIp + ":" + nodePort;
        this.peerId = toHash.hashCode();
        return peerId;
    }

    private void setNewPeerId(Integer newPeerId) {
        this.peerId = newPeerId;
    }

    public List<String> getStoredFilePaths() {
        return storedFilePaths;
    }

    public void appendToStoredFilePaths(String storedFilePaths) {
        this.storedFilePaths.add(storedFilePaths);
    }

    public void handleMessage(Message msg) {
        if (msg.getProtocol() == Protocol.NEW_PEER_ID) {
            this.setNewPeerId((Integer) msg.getPayload());
            System.out.println("New peer ID applied after collision resolution");
        }
        else if ((msg.getProtocol() == Protocol.BOOSTRAPPING_NODE_INFO)) {
            System.out.println("Now entering chord overlay");
            Peer bootstrapNode = (Peer) msg.getPayload();

            System.out.println("Boostrap node info "+ bootstrapNode.getPeerDescriptor());
            //TODO : Create finger table here now during join()
            joinUsingBootstrapNode(bootstrapNode);

        }
        else if ((msg.getProtocol() == Protocol.ADAM_NODE_INF0)) {
            System.out.println("First node in the chord");
            //no neighbors when adam node. (Only node in the ring)
            setNeighbors(this.getPeerDescriptor(), this.getPeerDescriptor());
            adjustAdamFingerTable();
        }

    }

    private void joinUsingBootstrapNode(Peer bootstrapNode) {
        try {
            //already established connection to node then
            if (tcpCache.containsKey(bootstrapNode.getPeerDescriptor())) {
                TCPConnection connection = tcpCache.get(bootstrapNode.getPeerDescriptor());

                //TODO: now start creating the FT
                initFingerTable();
                requestFingerTableFromBootstrapNode(connection);
            }
            else {
                Socket bootstrapNodeSocket = new Socket(bootstrapNode.getNodeIp(), bootstrapNode.getNodePort());
                TCPConnection connection = new TCPConnection(this, bootstrapNodeSocket);
                connection.startConnection();

                tcpCache.put(bootstrapNode.getPeerDescriptor(), connection);

                //TODO COMPLETE: now start creating the FT
                requestFingerTableFromBootstrapNode(connection);
                adjustFingerTableUsingBootstrapNode();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private void setNeighbors (String pred, String succ) {
        this.predecessorNode = pred;
        this.successorNode = succ;
    }

    //IMPORTANT: 1-Based indexing scheme for FT
    private void adjustFingerTableUsingBootstrapNode() {
        List <String> distinctRoutes = this.boostrapNodeFingerTable.findDistinctRoutableNodesInFT();
        if (distinctRoutes.size() == 1) {
            System.out.println("The Boostrap FT is at the End of Ring, thus routes to a single node");
            //more logic
            List <FingerTableEntry> candidateStorageEntries = boostrapNodeFingerTable.getEntriesSmallerThanNewNodeKey(this.getPeerId());

            //update FT in the predecessor node
            //migrate keys to the new node


        }
        else {

        }
    }

    //IMPORTANT: 1-Based indexing scheme for FT

    private void adjustAdamFingerTable() {
        int numFtRows = ChordConfig.NUM_PEERS;
        initFingerTable();

        //only node in the system, so succ, pred is itself
        getFingerTable().setPredecessorNodeDesc(this.getPeerDescriptor());
        getFingerTable().setSuccessorNodeDesc(this.getPeerDescriptor());

        for (int i = 1; i <= numFtRows; i++) {
            int index = (int) ((this.getPeerId() + (int) Math.pow(2, i - 1)) % Math.pow(2, numFtRows));
            FingerTableEntry finger = new FingerTableEntry(index, this.getPeerDescriptor());
            getFingerTable().addEntry(finger);
        }
        System.out.println("Finger table configured in adam node");
    }

    private void requestFingerTableFromBootstrapNode(TCPConnection connectionToBootstrapNode) {
        Message msg = new Message(Protocol.REQUEST_FINGER_TABLE, "");
        try {
            fingerTableRequestLatch = new CountDownLatch(1);
            connectionToBootstrapNode.getSenderThread().sendData(msg);
            fingerTableRequestLatch.await();
            System.out.println("Received finger table from bootstrap node");

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void sendFingerTable (TCPConnection connectionToRequestingNode) {
        if (this.fingerTable == null) {
            initFingerTable();
            //might need to populate the table too
        }
        try {
            connectionToRequestingNode.getSenderThread().sendData(this.fingerTable);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void receiveFingerTable (FingerTable boostrapNodeFingerTable) {
        this.boostrapNodeFingerTable = boostrapNodeFingerTable;
        this.fingerTableRequestLatch.countDown();
    }

    public void handleDiscoveryResponse(ServerResponse discoveryRes) {
        System.out.println("Received message from discovery with code "+ discoveryRes.getStatusCode().toString());
        System.out.println(discoveryRes.getAdditionalInfo());
    }

    private String setAndGetStorageDirectory() {
        this.fileStorageDirectory = ChordConfig.FILE_STORAGE_ROOT
                + "/tmp/"
                + this.nodePort;
        return this.fileStorageDirectory;
    }
    public void storeIncomingFileBytes(FileWrapper fileWrapper) {
        String filePathString = this.fileStorageDirectory + "/" + fileWrapper.getFileName();
        System.out.println("Writing file to path "+ filePathString);

        Path filePath = Paths.get(filePathString);
        try {
            FileUtils.createDirectoryIfNotExists(filePath);
            Files.write(filePath, fileWrapper.getFileBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //when download request reaches the terminal node
    public void sendFileOverWire(TCPConnection connection) {

    }

    public void downloadFileToDevice () {}


    /***
    Chord maintenance protocols
    */
    public void runMaintenance() {
        // Schedule the stabilize task
        scheduler.scheduleWithFixedDelay(() -> stabilize(), 0, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);

        // Schedule the fixFingers task
        scheduler.scheduleWithFixedDelay(() -> fixFingers(), 0, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);
    }
    private void stabilize() {
        System.out.println("Running stabilize() routine");
    }

    private void fixFingers() {
        System.out.println("Running fixFingers() routine");
    }
}
