package csx55.transport;

import csx55.chord.Discovery;
import csx55.chord.Peer;
import csx55.domain.ClientConnection;
import csx55.domain.Node;
import csx55.domain.Protocol;
import csx55.domain.RequestType;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

public class TCPReceiverThread implements Runnable{
    private Socket messageSource;
    private Node node;
    private DataInputStream din;

    private byte[] receivedPayload;

    private TCPConnection connection;

    private boolean terminated = false;

    public TCPReceiverThread (Node node, Socket socket, TCPConnection connection) throws IOException {
        messageSource = socket;
        this.node = node;
        din = new DataInputStream(socket.getInputStream());
        this.connection = connection;
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


    public void close() {
        try {
            this.din.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

