package csx55.domain;

import csx55.chord.Peer;

import java.io.*;


public class ClientConnection implements Serializable {
    private static final long serialversionUID = 1L;

    //private final int type = Protocol.CLIENT_CONNECTION;
    private Peer peerNode;

    private RequestType requestType;


    public ClientConnection(RequestType requestType, Peer peerNode) {
        this.requestType = requestType;
        this.peerNode = peerNode;
    }

    public ClientConnection() {

    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public Peer getPeerNode() {
        return peerNode;
    }

    public void setPeerNode(Peer peerNode) {
        this.peerNode = peerNode;
    }
}
