package csx55.domain;

import java.io.Serializable;

public class SuccessorNode extends ChordNode implements Serializable  {
    private static final long serialversionUID = 1L;

    public SuccessorNode(String descriptor, long peerId) {
        super(descriptor, peerId);
    }
}
