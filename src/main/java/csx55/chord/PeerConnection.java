package csx55.chord;

import csx55.transport.TCPConnection;

public class PeerConnection {
    private Peer peer;
    private TCPConnection connection;

    private final String connectionDescriptor;

    public PeerConnection(Peer peer, TCPConnection connection) {
        this.peer = peer;
        this.connection = connection;
        this.connectionDescriptor = peer != null ? peer.getPeerDescriptor() : null;
    }

    public Peer getPeer() {
        return peer;
    }

    public void setPeer(Peer peer) {
        this.peer = peer;
    }

    public TCPConnection getConnection() {
        return connection;
    }

    public void setConnection(TCPConnection connection) {
        this.connection = connection;
    }

    public String getConnectionDescriptor() {
        return connectionDescriptor;
    }
}
