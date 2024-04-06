package csx55.domain;

import java.io.Serializable;

public class ChordNode implements Serializable {
    private static final long serialversionUID = 1L;
    private String descriptor;

    private long peerId;

    public ChordNode(String descriptor, long peerId) {
        this.descriptor = descriptor;
        this.peerId = peerId;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }

    public long getPeerId() {
        return peerId;
    }

    public void setPeerId(long peerId) {
        this.peerId = peerId;
    }
}
