package csx55.chord;

import csx55.transport.TCPConnection;

public class PeerConnection {
    private Peer peer;
    private TCPConnection connection;

    public PeerConnection(Peer peer, TCPConnection connection) {
        this.peer = peer;
        this.connection = connection;
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
}
