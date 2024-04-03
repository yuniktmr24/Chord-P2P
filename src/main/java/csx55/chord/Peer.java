package csx55.chord;

import csx55.config.ChordConfig;
import csx55.domain.*;
import csx55.transport.TCPConnection;
import csx55.transport.TCPServerThread;
import csx55.util.FileUtils;
import csx55.util.FileWrapper;
import csx55.util.Tuple;

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

    private ChordNode predecessorNode;

    private ChordNode successorNode;


    private Map <String, TCPConnection> tcpCache = new HashMap<>();

    private String fileStorageDirectory;

    private List <String> storedFilePaths = new ArrayList<>();

    private transient TCPConnection discoveryConnection;

    private transient CountDownLatch fingerTableRequestLatch;

    private transient CountDownLatch ackLatch;

    private int next;

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

        String possibleSuccessor = possibleSuccessorEntry.getSuccessorNode().getDescriptor();
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
                    //now call getSuccessor to find actual storage node
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
        this.fingerTable = new FingerTable(this.getPeerId(), this.getPeerDescriptor());
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

        sb.append("Predecessor "+ predecessorNode.getDescriptor());
        sb.append(" | Successor" + successorNode.getDescriptor());
        sb.append("\n");
        sb.append("Predecessor id "+ predecessorNode.getPeerId());
        sb.append(" | Successor id " + successorNode.getPeerId());

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

    public void handleMessage(Message msg) throws InterruptedException {
        if (msg.getProtocol() == Protocol.NEW_PEER_ID) {
            this.setNewPeerId((Integer) msg.getPayload());
            System.out.println("New peer ID applied after collision resolution");
        }
        else if ((msg.getProtocol() == Protocol.BOOSTRAPPING_NODE_INFO)) {
            System.out.println("Now entering chord overlay");
            Peer bootstrapNode = (Peer) msg.getPayload();

            System.out.println("Boostrap node info "+ bootstrapNode.getPeerDescriptor());
            //TODO : Create finger table here now during join()
            initFingerTable();
            //TODO: stabilize boostrapNode before this and wait
            join(bootstrapNode);
            adjustFingerTable();
            //joinUsingBootstrapNode(bootstrapNode);

        }
        else if ((msg.getProtocol() == Protocol.ADAM_NODE_INF0)) {
            System.out.println("First node in the chord");
            //no neighbors when adam node. (Only node in the ring)
            //setNeighbors(new PredecessorNode(this.getPeerDescriptor(), this.getPeerId()),
                    //new SuccessorNode(this.getPeerDescriptor(), this.getPeerId()));
            adjustFingerTable();
            //no need to find successor, pred when only node in ring
            //join(this);
        }

    }

    private void joinUsingBootstrapNode(Peer bootstrapNode) {
        try {
            //already established connection to node then
            if (tcpCache.containsKey(bootstrapNode.getPeerDescriptor())) {
                TCPConnection connection = tcpCache.get(bootstrapNode.getPeerDescriptor());

                //TODO: now start creating the FT
                //initFingerTable();
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
    private void setNeighbors (PredecessorNode pred, SuccessorNode succ) {
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

    private void adjustFingerTable() {
        int numFtRows = ChordConfig.NUM_PEERS;
        initFingerTable();

        //only node in the system, so succ, pred is itself
        //getFingerTable().setPredecessorNode(new PredecessorNode(this.nodeIp, this.nodePort));
        //getFingerTable().setSuccessorNode(new SuccessorNode(this.nodeIp, this.nodePort));


        for (int i = 1; i <= numFtRows; i++) {
            int index = (int) ((this.getPeerId() + (1 << (i - 1))) & 0xFFFFFFFFL);

            FingerTableEntry finger = new FingerTableEntry(i, index,
                    successorNode == null ? this.getPeerDescriptor() : successorNode.getDescriptor() ,
                    successorNode == null ? this.getPeerId() : successorNode.getPeerId());
            getFingerTable().addEntry(finger);
        }
        if (successorNode != null) {
            getFingerTable().setSuccessorNode((SuccessorNode) successorNode);
        }
        if (predecessorNode != null) {
            getFingerTable().setPredecessorNode((PredecessorNode) predecessorNode);
        }
        stabilize();
        System.out.println("Finger table configured in adam node");
    }

    private void requestFingerTableFromBootstrapNode(TCPConnection connectionToBootstrapNode) {
        Message msg = new Message(Protocol.REQUEST_FINGER_TABLE, this.getPeerDescriptor() + ","  + this.getPeerId() );
        try {
            fingerTableRequestLatch = new CountDownLatch(1);
            connectionToBootstrapNode.getSenderThread().sendData(msg);
            fingerTableRequestLatch.await();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void sendFingerTable (TCPConnection connectionToRequestingNode) {
//        System.out.println("Received Finger Table request");
//        String newNodeDesc = msg.getPayload().toString().split(",")[0];
//        String newNodeId = msg.getPayload().toString().split(",")[1];
//        //as in adam node
//        //add new node to successor chain
//        if (predecessorNode == null && successorNode == null) {
//            successorNode = new SuccessorNode(newNodeDesc, Integer.parseInt(newNodeId));
//            this.fingerTable.setSuccessorNode((SuccessorNode) successorNode);
//        }

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

    public void receiveFingerTable (FingerTable boostrapNodeFingerTableRec) {
        this.boostrapNodeFingerTable = boostrapNodeFingerTableRec;
        this.fingerTableRequestLatch.countDown();
        System.out.println("Received finger table from bootstrap node");
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

    public void downloadFileToDevice () {

    }

    /***
    Chord maintenance protocols
    */
    public void runMaintenance() {
        // Schedule the stabilize task
        scheduler.scheduleWithFixedDelay(() -> stabilize(), 0, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);

        // Schedule the fixFingers task
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                fixFingers();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);
    }

    /***
     * Check and Fix the successor -> predecessor reference periodically
     * Also, update the FT[1] at the predecessor if needed
     * TODO: we also handle file migration here when successor is changed
     */
    private void stabilize() {
        if (successorNode != null && boostrapNodeFingerTable != null) {
            String successorIp = successorNode.getDescriptor().split(":")[0];
            int successorPort = Integer.parseInt(successorNode.getDescriptor().split(":")[1]);

            requestFingerTableFromBootstrapNode(getTCPConnection(tcpCache, successorIp, successorPort));
            ChordNode xNode = boostrapNodeFingerTable.getPredecessorNode();


            boolean currentNodeIsLastNode = false;
            if (this.getPeerId() > successorNode.getPeerId()) {
                currentNodeIsLastNode = true;
            }
            if ((xNode.getPeerId() > this.getPeerId()
                    && xNode.getPeerId() < successorNode.getPeerId())
                    || (currentNodeIsLastNode &&
                    (xNode.getPeerId() < successorNode.getPeerId()
                            && xNode.getPeerId() < this.getPeerId()))) {
                successorNode = xNode;
                //also maybe notify the other node via socket connection
            }
        }

        System.out.println("Running stabilize() routine");
        //call notify
        notify();
    }

    /***
     * Fix/update the finger table. Pick one random entry to update at a time.
     * Uses the lookup() to fix the entries
     */

    private void fixFingers() throws InterruptedException {
        System.out.println("Running fixFingers() routine");
        next = next + 1;
        if (next > ChordConfig.NUM_PEERS) {
            next = 1;
        }
        if (next == ChordConfig.NUM_PEERS) {
            //readjust keyspace ranges.
            getFingerTable().computeKeySpaceRanges();
        }
        //let's use successorNode as bootstrap node to fix FT
        if (getFingerTable() != null
                && successorNode != null
                //&& predecessorNode != null
                && successorNode.getPeerId() != this.getPeerId()
                //&& predecessorNode.getPeerId() != this.getPeerId()
        ) {
            getFingerTable().getFtEntries().get(next)
                    .setSuccessorNode(
                            findSuccessor(
                                    getPeerId() + (int) Math.pow(2, next - 1),
                                    successorNode.getDescriptor().split(":")[0],
                                    Integer.parseInt(successorNode.getDescriptor().split(":")[1])
                            ));
        }
    }

    /***
     * THE METHODS BELOW ARE IMPLEMENTED AS SPECIFIED IN THE ORIGINAL CHORD
     * PROTOCOL PAPER
     *
     * Source: https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf
     * Citation: Stoica, Ion, et al. "Chord: a scalable peer-to-peer lookup protocol for internet applications.
     * " IEEE/ACM Transactions on networking 11.1 (2003): 17-32.
     *
     */
    /***
     * Initialize the chord ring entry
     */
    private void create() {
        this.predecessorNode = null;
        this.successorNode = new SuccessorNode(this.getPeerDescriptor(), this.getPeerId());
    }

    /***
     * Join the overlay using bootstrap node
     */
    private void join (Peer bootstrapNode) throws InterruptedException {
        this.predecessorNode = null;
        this.successorNode = findSuccessor(this.getPeerId(), bootstrapNode.getNodeIp(), bootstrapNode.getNodePort());
    }


    /***
     * Notify module as specified in the chord protocol
     */
    private void notify(ChordNode nodeWhichThinksItsOurPredecessor) {
        //well the predecessor might be the last node in the chord ring
        //say in a 2 peer chord: with id 1 and 32
        //32 is predecessor of 1
        boolean predecessorIsLastNode = false;
        if (predecessorNode.getPeerId() > this.getPeerId()) {
            predecessorIsLastNode = true;
        }
        if (predecessorNode.getDescriptor() == null
                || nodeWhichThinksItsOurPredecessor.getPeerId() < predecessorNode.getPeerId()
                || (predecessorIsLastNode &&
                (nodeWhichThinksItsOurPredecessor.getPeerId() < predecessorNode.getPeerId()))) {
            predecessorNode = nodeWhichThinksItsOurPredecessor;
            //also maybe notify the other node via socket connection
        }
    }

    /***
     * Handle notification via notify(). This is in the peer that receives
     * message via socket
     *
     */
    private void handleNotify(ChordNode nodeWhichThinksItsOurPredecessor) {
        //well the predecessor might be the last node in the chord ring
        //say in a 2 peer chord: with id 1 and 32
        //32 is predecessor of 1
        boolean predecessorIsLastNode = false;
        if (predecessorNode.getPeerId() > this.getPeerId()) {
            predecessorIsLastNode = true;
        }
        if (predecessorNode.getDescriptor() == null
                || nodeWhichThinksItsOurPredecessor.getPeerId() < predecessorNode.getPeerId()
                ||
                //last node checks
                (predecessorIsLastNode
                        &&
                (nodeWhichThinksItsOurPredecessor.getPeerId() < predecessorNode.getPeerId()))
                || (predecessorIsLastNode
                &&
                (nodeWhichThinksItsOurPredecessor.getPeerId() > predecessorNode.getPeerId())
                && nodeWhichThinksItsOurPredecessor.getPeerId() < this.getPeerId())) {
            predecessorNode = nodeWhichThinksItsOurPredecessor;
        }

    }

    /***
     * Check predecessor. Ping predecessor occasionally. If it's down, then
     * well predecessor == null. Stablize should fix this later
     */
    private void checkPredecessor() {

    }

    //can't have null successors if more than 2 nodes in the overlay
    private void forceSuccessorUpdateIfNull() {
        String ip = boostrapNodeFingerTable.getCurrentNode().getDescriptor()
                .split(":")[0];
        int port = Integer.parseInt(boostrapNodeFingerTable.getCurrentNode().getDescriptor()
                .split(":")[1]);

        TCPConnection conn = getTCPConnection(tcpCache,ip,port);

        Message msg = new Message(Protocol.SEND_SUCCESSOR_INFO, this.getPeerDescriptor() + ","  + this.getPeerId());
        try {
            conn.getSenderThread().sendData(msg);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void ackSuccessorUpdateBecauseNull(TCPConnection conn, Message msg) {
        System.out.println("Received successor info");
        String newNodeDesc = msg.getPayload().toString().split(",")[0];
        String newNodeId = msg.getPayload().toString().split(",")[1];
//        //add new node to successor chain
        if (predecessorNode == null && successorNode == null) {
            //typically when two nodes in ring and we callback to the first node
            successorNode = new SuccessorNode(newNodeDesc, Integer.parseInt(newNodeId));
            predecessorNode = new PredecessorNode(newNodeDesc, Integer.parseInt(newNodeId));
            this.fingerTable.setSuccessorNode((SuccessorNode) successorNode);
            this.fingerTable.setPredecessorNode((PredecessorNode) predecessorNode);
        }
        //update FT while you're at it (???)
        List <FingerTableEntry> smallerEntries = this.fingerTable.
                getEntriesSmallerThanNewNodeKey(Integer.parseInt(newNodeId));

        int smallerEntriesIdx = 0;
        int lastKeyIsResponsibleForIndex = 0;
        for (FingerTableEntry entry: smallerEntries) {
            entry.setSuccessorNode((SuccessorNode) successorNode);
            smallerEntriesIdx++;
            //update the key space in largest finger smaller than the new key
            if (smallerEntriesIdx == smallerEntries.size() - 1) {
                entry.setKeySpaceRange(new Tuple(entry.getKey(),
                        successorNode.getPeerId()));
                entry.setSuccessorNode((SuccessorNode) successorNode);
                lastKeyIsResponsibleForIndex = entry.getIndex();
            }
        }
        FingerTableEntry nextEntryBeyondNode = this.fingerTable.getFtEntries()
                .get((lastKeyIsResponsibleForIndex + 1) % ChordConfig.NUM_PEERS);
        Tuple oldKeySpace = nextEntryBeyondNode.getKeySpaceRange();
        nextEntryBeyondNode.setKeySpaceRange(new Tuple(successorNode.getPeerId() + 1,
                oldKeySpace.getEnd()));
        //send updated finger table over the paired node
        //sendFingerTable(conn);
        Message ack = new Message(Protocol.ACK, "");
        try {
            conn.getSenderThread().sendData(ack);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * Ask bootstrap node to check for successor
     */
    private SuccessorNode findSuccessor(int targetKey, String bootstrapNodeIp, int bootstrapNodePort) throws InterruptedException {
        //first get FT from bootstrapNode;
        if (boostrapNodeFingerTable == null)
        {
            TCPConnection conn = getTCPConnection(tcpCache, bootstrapNodeIp, bootstrapNodePort);
            tcpCache.put(bootstrapNodeIp + ":" + bootstrapNodePort, conn);
            requestFingerTableFromBootstrapNode(conn);
        }
        //might need to add the last node in circle check
        boolean boostrapNodeIsLastNode = false;
        int bootstrapNodeId = boostrapNodeFingerTable.getCurrentNode().getPeerId();
        int successorNodeId = boostrapNodeFingerTable.getSuccessorNode() == null ?
                bootstrapNodeId :
                boostrapNodeFingerTable.getSuccessorNode().getPeerId();
        //second node in network case
        if (boostrapNodeFingerTable.getSuccessorNode() == null) {
            //refresh FT from boostrap node aka adam node
            ackLatch = new CountDownLatch(1);
            forceSuccessorUpdateIfNull();
            ackLatch.await();
            requestFingerTableFromBootstrapNode(getTCPConnection(tcpCache, bootstrapNodeIp, bootstrapNodePort));

            this.successorNode = new SuccessorNode(boostrapNodeFingerTable.getCurrentNode().getDescriptor(),
                    boostrapNodeFingerTable.getCurrentNode().getPeerId());
            this.predecessorNode = new PredecessorNode(boostrapNodeFingerTable.getCurrentNode().getDescriptor(),
                    boostrapNodeFingerTable.getCurrentNode().getPeerId());
            return (SuccessorNode) this.successorNode;
        }
        if (bootstrapNodeId > successorNodeId) {
            boostrapNodeIsLastNode = true;
        }
        boolean inBetweenTerminalBootstrapNodeAndSuccessor =
                (boostrapNodeIsLastNode
                && (targetKey > bootstrapNodeId))
                ||
                (boostrapNodeIsLastNode
                && (targetKey < bootstrapNodeId && targetKey <= successorNodeId));

        if (inBetweenTerminalBootstrapNodeAndSuccessor ||
                (targetKey > boostrapNodeFingerTable.getCurrentNode().getPeerId()
                        && targetKey <= boostrapNodeFingerTable.getSuccessorNode().getPeerId())) {
            successorNode = new SuccessorNode(
                    boostrapNodeFingerTable.getSuccessorNode().getDescriptor(),
                    boostrapNodeFingerTable.getSuccessorNode().getPeerId());
            return boostrapNodeFingerTable.getSuccessorNode();
        }
        else {
            ChordNode closestPrecedingNode = getClosestPrecedingNode(targetKey, boostrapNodeFingerTable.getCurrentNode());
            String ip = closestPrecedingNode.getDescriptor().split(":")[0];
            int port = Integer.parseInt(
                    closestPrecedingNode.getDescriptor()
                            .split(":")[1]);

            return findSuccessor(targetKey, ip, port);
        }
    }

    private ChordNode getClosestPrecedingNode(int targetKey, ChordNode currentNode) {
        for (int idx = ChordConfig.NUM_PEERS; idx >= 1; idx--) {
            SuccessorNode currentSuccessor = boostrapNodeFingerTable.getFtEntries().get(idx - 1).getSuccessorNode();
            int currentSuccessorId = currentSuccessor.getPeerId();

            boolean isInCircularInterval = (currentNode.getPeerId() < targetKey) ?
                    (currentSuccessorId > currentNode.getPeerId() && currentSuccessorId < targetKey) : // Normal case
                    (currentSuccessorId > currentNode.getPeerId() || currentSuccessorId < targetKey); // Circular case

            if (isInCircularInterval) {
                return currentSuccessor;
            }
            //account for terminal node problem here as well TODO
            if (currentSuccessorId > currentNode.getPeerId() && currentSuccessorId < targetKey) {
                return currentSuccessor;
            }
        }
        return currentNode;
    }

    public TCPConnection getTCPConnection(Map<String, TCPConnection> tcpCache, String bootstrapNodeIp, int bootstrapNodePort) {
        TCPConnection conn = null;
        if (tcpCache.containsKey(bootstrapNodeIp+ ":" + bootstrapNodePort)) {
            conn = tcpCache.get(bootstrapNodeIp+ ":" + bootstrapNodePort);
        }
        else {
            try {
                Socket clientSocket = new Socket(bootstrapNodeIp, bootstrapNodePort);
                conn = new TCPConnection(this, clientSocket);
                tcpCache.put(bootstrapNodeIp+ ":" + bootstrapNodePort, conn);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (!conn.isStarted()) {
            conn.startConnection();
        }
        return conn;
    }

    public void receiveAck() {
        this.ackLatch.countDown();
    }
}
