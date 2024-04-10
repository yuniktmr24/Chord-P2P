package csx55.domain;

import java.io.Serializable;

public class PredecessorNode extends ChordNode implements Serializable {
    private static final long serialversionUID = 1L;
    public PredecessorNode(String descriptor, long peerId) {
        super(descriptor, peerId);
    }
}
