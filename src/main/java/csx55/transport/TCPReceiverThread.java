package csx55.transport;

import csx55.chord.Discovery;
import csx55.chord.FingerTable;
import csx55.chord.Peer;
import csx55.domain.*;
import csx55.util.FileWrapper;

import java.io.*;
import java.net.Socket;

public class TCPReceiverThread implements Runnable {
    private Socket messageSource;
    private Node node;
    private ObjectInputStream ois;

    private byte[] receivedPayload;

    private TCPConnection connection;

    private boolean terminated = false;

    public TCPReceiverThread (Node node, Socket socket, TCPConnection connection) throws IOException {
        try {
            messageSource = socket;
            this.node = node;
            ois = new ObjectInputStream(socket.getInputStream());
            this.connection = connection;
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void terminateReceiver(){
        terminated = true;
    }

    public byte[] getReceivedPayload() {
        return receivedPayload;
    }

    private void setReceivedPayload(byte[] receivedPayload) throws IOException {
        this.receivedPayload = receivedPayload;
    }

    @Override
    public void run() {
        while (true) {
            Serializable object;
            try {
                object = readObject(ois);
                if (object instanceof FileWrapper && node instanceof Peer) {
                    FileWrapper file = (FileWrapper) object;
                    // Here, you can process the file bytes, e.g., save them to a file
                    ((Peer)node).storeIncomingFileBytes(file);
                }
                if (node instanceof Discovery) {
                    //could be made into an eventFactory
                    if (object instanceof ClientConnection) {
                        ((Discovery) node).handleJoin(messageSource, (ClientConnection) object, connection);
                    }
                }
                //peer node
                else if (node instanceof Peer ){
                    if (object instanceof Message) {
                        Message msg = (Message)object;
                        if (msg.getProtocol() == Protocol.REQUEST_FINGER_TABLE) {
                            ((Peer) node).sendFingerTable(connection);
                        }
                        else if (msg.getProtocol() == Protocol.SEND_SUCCESSOR_INFO) {
                            ((Peer) node).ackSuccessorUpdateBecauseNull(connection, msg);
                        }
                        else if (msg.getProtocol() == Protocol.ACK) {
                            ((Peer) node).receiveAck();
                        }
                        else {
                            ((Peer) node).handleMessage(msg);
                        }
                    } else if (object instanceof ServerResponse) {
                        ((Peer) node).handleDiscoveryResponse((ServerResponse)object);
                    } else if (object instanceof FingerTable) {
                        ((Peer) node).receiveFingerTable((FingerTable) object);
                    }
                }
            } catch (Exception ex) {
                this.close();
            }
        }
    }

    private Serializable readObject (ObjectInputStream ois) {
        try {
            return (Serializable) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    /*
    @Deprecated
    public void runDeprecated() {
        //keep listening until socket is open
        while (!messageSource.isClosed()) {
            try {
                int dataLength = din.readInt();
                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);
                setReceivedPayload(data);
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                DataInputStream din = new DataInputStream(new BufferedInputStream(byteArrayInputStream));
                int domainType = din.readInt();

                if (node instanceof Discovery) {
                    try {
                        if (domainType == Protocol.CLIENT_CONNECTION) { //could refactor these numbers to a protocol class
                           // ClientConnection conn = new ClientConnection().unmarshal(data);
                            //((Discovery) node).handleJoin(messageSource, conn, connection);
                        }

                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                else if (node instanceof Peer) {
                    //can be peer to
                    try {
                        if (domainType == Protocol.JOIN_CHORD_REQUEST) {
                            //ClientConnection peerConnection = new ClientConnection().unmarshal(data);
                            //if (peerConnection.getRequestType().equals(RequestType.REQUEST_TOTAL_TASK_INFO)) {
                               // ((Peer) node).sendTaskInfo(peerConnection, connection);
                            //}
                        }
                    }
                    catch (Exception ex) {
                        //yeah well move on to the other "exceptional" payload type
                       ex.printStackTrace();
                       this.close();
                        //deserializeBytes(data);
                    }

                }
            } catch (Exception e) {
                System.out.println("Error in node "+ node.toString());
                this.close();
                e.printStackTrace();
            }
        }
    }

     */


    public void close() {
        try {
            this.ois.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

