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
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static csx55.util.FileUtils.removeFileExtension;

public class Peer extends Node implements Serializable {
    private transient final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private static final long serialversionUID = 1L;
    private static final Logger logger = Logger.getLogger(Peer.class.getName());

    private FingerTable fingerTable;

    private transient FingerTable boostrapNodeFingerTable;

    private List<String> fileNames;

    private String nodeIp;

    private int nodePort;

    private long peerId = -1;

    private ChordNode predecessorNode;

    private ChordNode successorNode;


    private Map <String, TCPConnection> tcpCache = new HashMap<>();

    private String fileStorageDirectory;

    private List <String> storedFilePaths = new ArrayList<>();

    private transient TCPConnection discoveryConnection;

    private transient CountDownLatch fingerTableRequestLatch;

    private transient Socket socketToRegistry;

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
            //TODO: DLELTE THIS before PROD
            if (args.length > 0) {
                peer.peerId = Long.parseLong(args[0]);
                peer.setAndGetStorageDirectory();
            }

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
            if (this.socketToRegistry == null) {
                this.socketToRegistry = socketToRegistry;
            }
            ClientConnection joinChord = new ClientConnection(RequestType.JOIN_CHORD, this);

            TCPConnection connection = new TCPConnection(peer, socketToRegistry);

            connection.getSenderThread().sendObject(joinChord);
            connection.startConnection();
            //connection.getSenderThread().sendData();
            if (this.discoveryConnection == null) {
                this.discoveryConnection = connection;
            }
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
        //TODO : remove this deubg code before prod
        String fileNameWithExtension = file.getFileName();
        String fileNameWithoutExtension = removeFileExtension(fileNameWithExtension);

        long dataKey = Math.abs(fileNameWithExtension.hashCode());

        if (ChordConfig.DEBUG_MODE) {
            dataKey = Long.parseLong(fileNameWithoutExtension);
        }

        System.out.println("Uploaded File hashcode is "+ dataKey);

        //got to send it to some other node
        //request FT from other node and proceed from there

        SuccessorNode successor = findSuccessorNode(dataKey,
                this.nodeIp,
                this.nodePort);
        System.out.println("Storage node is " + successor.getPeerId());

        if (successor.getPeerId() == this.getPeerId()) {
            //store at self //SAME NODE STORAGE
            storeIncomingFileBytes(file);
        }
        else {
            TCPConnection connToSuccessor = getTCPConnection(tcpCache,
                    successor.getDescriptor().split(":")[0],
                    Integer.parseInt(successor.getDescriptor().split(":")[1]));

            try {
                connToSuccessor.getSenderThread().sendData(file);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void download(String fileToDownloadWithExtension) {
        String file = FileUtils.removeFileExtension(fileToDownloadWithExtension);
        //long key = Long.parseLong(file); //TODO : fix this before prod //file.hashCode();
        long key = fileToDownloadWithExtension.hashCode();

        if (ChordConfig.DEBUG_MODE) {
            key = Long.parseLong(file);
        }

        System.out.println("To be downloaded File hashcode is "+ key);
        SuccessorNode successor = findSuccessorNode(key, this.nodeIp, this.nodePort);

        System.out.println("Storage node is " + successor.getPeerId());

        if (successor.getPeerId() == this.getPeerId()) {
            //download from self //SAME NODE STORAGE
            downloadFileBytesFromOwnStorage(fileToDownloadWithExtension);
        }
        else {
            //make download request payloadf
            Message downloadRequest = new Message(Protocol.DOWNLOAD_REQUEST, fileToDownloadWithExtension);

            TCPConnection connToSuccessor = getTCPConnection(tcpCache,
                    successor.getDescriptor().split(":")[0],
                    Integer.parseInt(successor.getDescriptor().split(":")[1]));

            try {
                connToSuccessor.getSenderThread().sendData(downloadRequest);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void acknowledgeDownloadRequest (Message msg, TCPConnection conn) {
        //fetch file and then send it over the wire as Download_Response payload type
        // and maybe set object to be of type FileWrapper
        System.out.println("Received download request");
        //first Find file, create wrapper, send it over
        Path storageRoot = Paths.get(ChordConfig.FILE_STORAGE_ROOT + "/" + this.peerId);

        try {
            // Search for the file in the given directory
            Optional<Path> foundFile = Files.walk(storageRoot)
                    .filter(p -> p.getFileName().toString().equals(msg.getPayload()))
                    .findFirst();

            if (foundFile.isPresent()) {
                Path file = foundFile.get();
                // Read all bytes from the found file
                byte[] fileBytes = Files.readAllBytes(file);
                // Create a new FileWrapper with the file's bytes and name
                FileWrapper fileWrapper = new FileWrapper(fileBytes, file.getFileName().toString());
                System.out.println("FileWrapper created for: " + file);

                Message sendFile = new Message(Protocol.DOWNLOAD_RESPONSE, fileWrapper);
                try {
                    conn.getSenderThread().sendObject(sendFile);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            } else {
                System.out.println("File not found: ");
                Message sendFile = new Message(Protocol.DOWNLOAD_RESPONSE, "File not found");
                try {
                    conn.getSenderThread().sendObject(sendFile);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            System.err.println("Error while searching for file or reading file: " + e.getMessage());
            System.out.println("File not found: ");
            Message sendFile = new Message(Protocol.DOWNLOAD_RESPONSE, "File not found");
            try {
                conn.getSenderThread().sendObject(sendFile);
            } catch (InterruptedException | IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void downloadFileBytesFromOwnStorage(String file) {
        //yeah well lets copy it to the download directory then
        Path sourcePath = Paths.get(ChordConfig.FILE_STORAGE_ROOT + "/" + this.getPeerId(), file);
        Path destinationPath = Paths.get(ChordConfig.FILE_DOWNLOAD_ROOT, file);

        try {
            // Ensure the download directory exists
            Files.createDirectories(destinationPath.getParent());

            // Copy file from storage to download directory
            Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("File successfully copied to " + destinationPath);
        } catch (IOException e) {
            System.err.println("Error copying file: " + e.getMessage());
        }
    }

    public void downloadIncomingFileBytes(Message msg) {
        if (msg.getPayload() instanceof String) {
            System.out.println(msg.getPayload());
        }
        else {
            FileWrapper incomingFile = (FileWrapper) msg.getPayload();

            // Retrieve the directory path from ChordConfig
            Path directoryPath = Paths.get(ChordConfig.FILE_DOWNLOAD_ROOT);
            Path filePath = directoryPath.resolve(incomingFile.getFileName());

            try {
                // Create directories if they do not exist
                Files.createDirectories(directoryPath);
                // Write bytes to file, creating the file if it doesn't exist or overwriting existing file
                Files.write(filePath, incomingFile.getFileBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                System.out.println("File saved successfully to " + filePath);
            } catch (IOException e) {
                System.err.println("Error writing file: " + e.getMessage());
            }
        }
    }

    public void checkSuccessorForFileStorage() {
        Path dir = Paths.get(this.fileStorageDirectory);
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(path -> {
                byte [] fileBytes = null;
                try {
                    fileBytes = Files.readAllBytes(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                String fileName = path.getFileName().toString();
                long key = fileName.hashCode();
                if (ChordConfig.DEBUG_MODE) {
                    key = Long.parseLong(removeFileExtension(fileName));
                }
                //System.out.println(fileName + " -> HashCode: " + key);
                SuccessorNode successor = findSuccessorNode(key, this.nodeIp, this.nodePort);
                System.out.println(fileName + " -> HashCode: " + key + "| Successor -> "+ successor.getPeerId());
                //transfer file over if successor is different than self
                if (successor.getPeerId() != this.getPeerId()) {
                    FileWrapper file = new FileWrapper(fileBytes, fileName);
                    TCPConnection connToSuccessor = getTCPConnection(tcpCache,
                            successor.getDescriptor().split(":")[0],
                            Integer.parseInt(successor.getDescriptor().split(":")[1]));

                    try {
                        connToSuccessor.getSenderThread().sendData(file);
                        //TODO: well actually might be smarter to delete if the successor sends ack that it has done storage
                        Files.deleteIfExists(path);
                        System.out.println("Transferred file to new successor -> "+ successor.getPeerId());
                    } catch (InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (IOException e) {
            System.out.println("Error reading directory");
            e.printStackTrace();
        }
    }


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


    public long getPeerId() {
        this.peerId = peerId == -1 ? setAndGetPeerId() : this.peerId;
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
                boolean containsSpace = false,
                        validUploadFilesCmd = false,
                        validDownloadFilesCmd = false,
                        validCheckSuccessorCmd = false;
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
                    else if (userInput.startsWith(UserCommands.CHECK_SUCCESSOR.getCmd()) ||
                            userInput.toUpperCase().contains("check_successor") ||
                            userInput.startsWith(String.valueOf(UserCommands.CHECK_SUCCESSOR.getCmdId()))) {
                        validCheckSuccessorCmd = true;
                        long key = Long.parseLong(userInput.split(" ")[1]);
                        SuccessorNode successor = findSuccessorNode(key, this.nodeIp, this.nodePort, true);
                        System.out.println(successor.getPeerId());
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
                    Path dir = Paths.get(this.fileStorageDirectory);
                    try (Stream<Path> stream = Files.list(dir)) {
                        stream.forEach(System.out::println);
                    } catch (IOException e) {
                        System.out.println("Error reading directory");
                        e.printStackTrace();
                    }
                    //FileUtils.printFileNamesWithHashCode(this.fileStorageDirectory);
                    checkSuccessorForFileStorage();
                    //node.getStoredFilePaths().forEach(System.out::println);
                }
                //for testing
                else if (userInput.equals(UserCommands.FINGER_TABLE.getCmd()) || userInput.equals(String.valueOf(UserCommands.FINGER_TABLE.getCmdId()))) {
                    System.out.println("Current Node ID : "+ this.getPeerId());
                    node.getFingerTable().printFingerTable();
                }
                else if (userInput.equals(UserCommands.FIX_FINGER.getCmd()) || userInput.equals(String.valueOf(UserCommands.FIX_FINGER.getCmdId()))) {
                    fixFinger();
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


    public long setAndGetPeerId() {
        String toHash = nodeIp + ":" + nodePort;
        this.peerId = Math.abs(toHash.hashCode());
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
            //rejoin chord with new peer ID
            ClientConnection joinChord = new ClientConnection(RequestType.JOIN_CHORD, this);
            try {
                this.discoveryConnection.getSenderThread().sendObject(joinChord);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else if ((msg.getProtocol() == Protocol.BOOSTRAPPING_NODE_INFO)) {
            System.out.println("Now entering chord overlay");
            Peer bootstrapNode = (Peer) msg.getPayload();

            System.out.println("Bootstrap node info "+ bootstrapNode.getPeerId());
            //TODO : Create finger table here now during join()
            initFingerTable();
            //TODO: stabilize boostrapNode before this and wait
            join(bootstrapNode);
            adjustFingerTable();
            updateFingerTable();
            //fixFinger();
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

    //fixes the local Finger table

    private void fixFinger() {
        if (successorNode != null && successorNode.getPeerId() > this.peerId) {
            System.out.println("Fixing fingers locally");
            for (int i = ChordConfig.NUM_PEERS; i >= 1; i--) {
                FingerTableEntry entry = this.fingerTable.getFtEntries().get(i - 1);
                SuccessorNode successor = findSuccessorNode(entry.getKey(), this.nodeIp, this.nodePort);
                //System.out.println("Successor for "+ entry.getKey() + " is "+ successor.getPeerId());

                String successorDesc = successor.getDescriptor();
                long succId = successor.getPeerId();

                this.fingerTable.getFtEntries().get(i - 1).setSuccessorNode(new SuccessorNode(successorDesc, succId));
                this.fingerTable.getFtEntries().get(i - 1).setSuccessorNodeDesc(successorDesc);
                this.fingerTable.getFtEntries().get(i - 1).setSuccessorNodeId(succId);
            }
        }
    }


    private void setNeighbors (PredecessorNode pred, SuccessorNode succ) {
        this.predecessorNode = pred;
        this.successorNode = succ;
    }


    //IMPORTANT: 1-Based indexing scheme for FT

    private void adjustFingerTable() {
        int numFtRows = ChordConfig.NUM_PEERS;
        initFingerTable();

        //only node in the system, so succ, pred is itself
        //getFingerTable().setPredecessorNode(new PredecessorNode(this.nodeIp, this.nodePort));
        //getFingerTable().setSuccessorNode(new SuccessorNode(this.nodeIp, this.nodePort));

        if (successorNode == null) {
            successorNode = new SuccessorNode(this.getPeerDescriptor(), this.getPeerId());
        }

        for (int i = 1; i <= numFtRows; i++) {
            long index = (long) ((this.getPeerId() + Math.pow(2, i - 1)) % Math.pow(2, ChordConfig.NUM_PEERS));

            FingerTableEntry finger = new FingerTableEntry(i, index,
                     successorNode.getDescriptor(),
                     successorNode.getPeerId());
            getFingerTable().addEntry(finger);
        }
        if (successorNode != null) {
            getFingerTable().setSuccessorNode((SuccessorNode) successorNode);
        }
        if (predecessorNode != null) {
            getFingerTable().setPredecessorNode((PredecessorNode) predecessorNode);
        }
        //stabilize();
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
        //System.out.println("Received finger table from bootstrap node");
    }

    public void handleDiscoveryResponse(ServerResponse discoveryRes) {
        System.out.println("Received message from discovery with code "+ discoveryRes.getStatusCode().toString());
        System.out.println(discoveryRes.getAdditionalInfo());
    }

    private String setAndGetStorageDirectory() {
        this.fileStorageDirectory = ChordConfig.FILE_STORAGE_ROOT + "/"
                + this.peerId;
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
        //scheduler.scheduleWithFixedDelay(() -> stabilize(), 0, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(() -> fixFinger(), 15, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);

        /*
        // Schedule the fixFingers task
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                fixFingers();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 0, ChordConfig.MAINTENANCE_INTERVAL, TimeUnit.SECONDS);
        */
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
     * Join the overlay using bootstrap node
     */
    private void join (Peer bootstrapNode) throws InterruptedException {
        this.predecessorNode = null;
        //this.successorNode = findSuccessor(this.getPeerId(), bootstrapNode.getNodeIp(), bootstrapNode.getNodePort());
        this.successorNode = findSuccessorNode(this.getPeerId(), bootstrapNode.getNodeIp(), bootstrapNode.getNodePort());

        TCPConnection conn = getTCPConnection(tcpCache,
                this.successorNode.getDescriptor().split(":")[0],
                Integer.parseInt(this.successorNode.getDescriptor().split(":")[1]));
        tcpCache.putIfAbsent(this.successorNode.getDescriptor(), conn);
        requestFingerTableFromBootstrapNode(conn);

        this.predecessorNode = boostrapNodeFingerTable.getPredecessorNode();
        //after this contact predecessor of successor and make them update thier
        //successor pointter
        StabilizationPayload payload = new StabilizationPayload(new ChordNode(
                this.getPeerDescriptor(),
                this.getPeerId()
        ));

        conn.getSenderThread().sendData(payload);

        //update everything in FT to point to successor node if key smaller than successor node
        //successor node is finalized here, so we don't do recursive lookups again
        //check for circular links as well
        updateFingerTable();
    }

    private synchronized void updateFingerTable () {
        if (isCircular(this.getPeerId(), successorNode.getPeerId())) {
            //ftEntries with keys greater than peerId
            //ftEntries with keys between 0 and equals to successorNode.getPeerId()
            List <FingerTableEntry> fingersToUpdate = fingerTable.getToBeModifiedFingersCircular(this.getPeerId(), successorNode.getPeerId());
            for (FingerTableEntry updatedFinger: fingersToUpdate) {
                long fingerIndex = updatedFinger.getIndex();
                updatedFinger.setSuccessorNode((SuccessorNode) successorNode);
            }
        }
        else {
            List <FingerTableEntry> fingersToUpdate = fingerTable.getEntriesSmallerThanEqualToNewNodeKey(successorNode.getPeerId());
            for (FingerTableEntry updatedFinger: fingersToUpdate) {
                long fingerIndex = updatedFinger.getIndex();
                updatedFinger.setSuccessorNode((SuccessorNode) successorNode);
            }
        }
    }

    //JOIN -> contactPredecessorToStabize both need FT updates.
    //when current node role is successor to another node aka xNode
    //note the other node still has to deal with updating its pred, succ
    public void contactPredecessorToStabilize (ChordNode xNode) {
        //cold start problem
        if (predecessorNode == null) {
            predecessorNode = new PredecessorNode(xNode.getDescriptor(), xNode.getPeerId());
            fingerTable.setPredecessorNode((PredecessorNode) predecessorNode);
            if (successorNode.getDescriptor().equals(this.getPeerDescriptor())) {
                successorNode = new SuccessorNode(xNode.getDescriptor(), xNode.getPeerId());
                fingerTable.setSuccessorNode((SuccessorNode) successorNode);
                //notify that xNode so that they can now update their predecessor
                TCPConnection xNodeConn = getTCPConnection(tcpCache, xNode.getDescriptor().split(":")[0], Integer.parseInt(xNode.getDescriptor().split(":")[1]));
                tcpCache.putIfAbsent(xNode.getDescriptor(), xNodeConn);
                notifySuccessor(xNodeConn);
            }
        }
        else {
            //well since the predecessor contacted us, we'll update our pred pointer first
            //now contact our old predecessor and do actual stabilization in there
            TCPConnection predConn = getTCPConnection(tcpCache, predecessorNode.getDescriptor().split(":")[0],
                    Integer.parseInt(predecessorNode.getDescriptor().split(":")[1]));
            tcpCache.putIfAbsent(predecessorNode.getDescriptor(), predConn);

            PredecessorNode newPredecessor = new PredecessorNode(xNode.getDescriptor(), xNode.getPeerId());
            //notifyPredecessor(predConn, (PredecessorNode) predecessorNode);
            notifyPredecessor(predConn, newPredecessor);

            this.predecessorNode = newPredecessor;
            fingerTable.setPredecessorNode((PredecessorNode) predecessorNode);

        }
        //update FT TODO
        //update everything in FT to point to successor node if key smaller than successor node
        //successor node is finalized here, so we don't do recursive lookups again
        //check for circular links as well
        //updateFingerTable();
    }

    private void notifySuccessor(TCPConnection successorConn) {
        UpdatePredecessorPayload payload = new UpdatePredecessorPayload(new PredecessorNode(
                this.getPeerDescriptor(), this.getPeerId()
        ));
        try {
            successorConn.getSenderThread().sendData(payload);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    //notifyPredecessor -> handleSuccessorComms - part of actual stabilize
    private void notifyPredecessor(TCPConnection predConn, PredecessorNode predInfo) {
        UpdateSuccessorPayload payload = new UpdateSuccessorPayload(new SuccessorNode(
                predInfo.getDescriptor(), predInfo.getPeerId()
        ));
        try {
            predConn.getSenderThread().sendData(payload);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void handleSuccessorComms (UpdateSuccessorPayload payload) {
        System.out.println("Received comms from predecessor");
        SuccessorNode succ = new SuccessorNode(payload.getxNode().getDescriptor(), payload.getxNode().getPeerId());
        this.successorNode = succ;
        fingerTable.setSuccessorNode(succ);
        //lets update our FT since our successor pointer may have changed
        updateFingerTable();
        //TODO lets make our successor update its predecessor then
//        TCPConnection connSucc  = getTCPConnection(tcpCache, payload.getxNode().getDescriptor().split(":")[0],
//                Integer.parseInt(payload.getxNode().getDescriptor().split(":")[1]));
//        notifySuccessor(connSucc);
        //TODO: migrate data to successor
    }


    //when predecessor messages you, handle it
    public void handlePredecessorComms (UpdatePredecessorPayload payload) {
        System.out.println("Received comms from successor ");
        PredecessorNode pred = new PredecessorNode(payload.getxNode().getDescriptor(), payload.getxNode().getPeerId());
        this.predecessorNode = pred;
        fingerTable.setPredecessorNode(pred);
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

    private SuccessorNode findSuccessorNode (long targetId, String bootstrapNodeIp, int bootstrapNodePort
            ) {
        return findSuccessorNode(targetId, bootstrapNodeIp, bootstrapNodePort, false);
    }

    private SuccessorNode findSuccessorNode (long targetId, String bootstrapNodeIp, int bootstrapNodePort, boolean printHops) {
        if (boostrapNodeFingerTable == null
                || (!(bootstrapNodeIp + ":" + bootstrapNodePort).equals(boostrapNodeFingerTable.getCurrentNode().getDescriptor())
        )) //stale fingerTable. reload ft for new load)
        {
            TCPConnection conn = getTCPConnection(tcpCache, bootstrapNodeIp, bootstrapNodePort);
            tcpCache.put(bootstrapNodeIp + ":" + bootstrapNodePort, conn);
            requestFingerTableFromBootstrapNode(conn);
            if (printHops) {
                System.out.println("Routed to " + boostrapNodeFingerTable.getCurrentNode().getPeerId());
            }
        }
        long bootstrapNodeId = boostrapNodeFingerTable.getCurrentNode().getPeerId();
        long successorId = boostrapNodeFingerTable.getSuccessorNode().getPeerId();

        if (nodeBetween (targetId, bootstrapNodeId, successorId)) {
            return new SuccessorNode(boostrapNodeFingerTable.getSuccessorNode().getDescriptor(),
                    boostrapNodeFingerTable.getSuccessorNode().getPeerId());
        }
        else {
            ChordNode precedingNode = boostrapNodeFingerTable.findClosestPrecedingNode(targetId);
            //BREAK RECURSION HERE
            if (precedingNode.getPeerId() == boostrapNodeFingerTable.getCurrentNode().getPeerId()) {
                return new SuccessorNode(boostrapNodeFingerTable.getCurrentNode().getDescriptor(),
                        boostrapNodeFingerTable.getCurrentNode().getPeerId());
            }
            return findSuccessorNode(targetId, precedingNode.getDescriptor().split(":")[0],
                    Integer.parseInt(precedingNode.getDescriptor().split(":")[1]));
        }
    }

    private boolean nodeBetween (long newnodeId, long bootstrapnodeId, long successornodeId) {
        if (bootstrapnodeId < successornodeId) {
            return bootstrapnodeId < newnodeId && newnodeId < successornodeId;
        }
        else {
            return newnodeId > bootstrapnodeId || newnodeId < successornodeId;
        }
    }


    private SuccessorNode findSuccessor (long targetKey, String bootstrapNodeIp, int bootstrapNodePort) {
        if (boostrapNodeFingerTable == null
        || (!(bootstrapNodeIp + ":" + bootstrapNodePort).equals(boostrapNodeFingerTable.getCurrentNode().getDescriptor())
            )) //stale fingerTable. reload ft for new load)
        {
            TCPConnection conn = getTCPConnection(tcpCache, bootstrapNodeIp, bootstrapNodePort);
            tcpCache.put(bootstrapNodeIp + ":" + bootstrapNodePort, conn);
            requestFingerTableFromBootstrapNode(conn);
        }
        FingerTableEntry ftEntry =  boostrapNodeFingerTable.lookup(targetKey);
        //TODO : account for circular connection
        SuccessorNode bootstrapSuccessor = ftEntry.getSuccessorNode();

        boolean bootstrapNodeIsSuccessor = false;
        //successor node is the current node
        if (bootstrapSuccessor.getPeerId() == boostrapNodeFingerTable.getCurrentNode().getPeerId()) {
            bootstrapNodeIsSuccessor = true;
            return new SuccessorNode(bootstrapSuccessor.getDescriptor(),
                    bootstrapSuccessor.getPeerId());
        }
        //check circle more comprehensively. could be two nodes in ring and they just call each other's FT
        //all the time. Break that cycle
        else if (isCircular(boostrapNodeFingerTable.getCurrentNode().getPeerId(),
                bootstrapSuccessor.getPeerId())
            || isCircular(bootstrapSuccessor.getPeerId(),
                    boostrapNodeFingerTable.getCurrentNode().getPeerId())) {
            return new SuccessorNode(boostrapNodeFingerTable.getCurrentNode().getDescriptor(),
                    boostrapNodeFingerTable.getCurrentNode().getPeerId());
        }
        //first successor of lookup itself is greater than key then that's the successor
        else if (ftEntry.getIndex() == 1) {
            //TODO : What if predecessors are greater than new key?
            return new SuccessorNode(boostrapNodeFingerTable.getSuccessorNode().getDescriptor(),
                    boostrapNodeFingerTable.getSuccessorNode().getPeerId());
        }
        else {
            //update bootstrap FT table since we're routing to new node
            TCPConnection conn = getTCPConnection(tcpCache, bootstrapSuccessor.getDescriptor().split(":")[0],
                    Integer.parseInt(bootstrapSuccessor.
                            getDescriptor().split(":")[1]));
            requestFingerTableFromBootstrapNode(conn);
            return findSuccessor(targetKey,
                    bootstrapSuccessor.getDescriptor().split(":")[0],
                    Integer.parseInt(bootstrapSuccessor.
                            getDescriptor().split(":")[1]));
        }
        //after finding proper successor: TODO
        // 1) we should update our FT appropriately
        // 2) contact predecessor of successor and make them update its successor
        // 3) that successor.predecessor should also notify us to update our predecessor
    }

    public boolean isCircular(long targetId, long successorId) {
        if (targetId > successorId) {
            return true;
        }
        return false;
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



    /***
     * Check and Fix the successor -> predecessor reference periodically
     * Also, update the FT[1] at the predecessor if needed
     * TODO: we also handle file migration here when successor is changed
     */
    /*
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

    /*
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
     * Initialize the chord ring entry
     */
    /*
    private void create() {
        this.predecessorNode = null;
        this.successorNode = new SuccessorNode(this.getPeerDescriptor(), this.getPeerId());
    }
    */


}
