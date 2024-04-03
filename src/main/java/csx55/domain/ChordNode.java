package csx55.domain;

import java.io.Serializable;

public class ChordNode implements Serializable {
    private static final long serialversionUID = 1L;
    private String descriptor;

    private int peerId;

    public ChordNode(String descriptor, int peerId) {
        this.descriptor = descriptor;
        this.peerId = peerId;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }

    public int getPeerId() {
        return peerId;
    }

    public void setPeerId(int peerId) {
        this.peerId = peerId;
    }
}
