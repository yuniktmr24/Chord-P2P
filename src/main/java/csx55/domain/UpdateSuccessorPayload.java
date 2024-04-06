package csx55.domain;

import java.io.Serializable;

public class UpdateSuccessorPayload implements Serializable {
    private static final long serialversionUID = 1L;
    public ChordNode xNode;

    public UpdateSuccessorPayload(ChordNode xNode) {
        this.xNode = xNode;
    }

    public ChordNode getxNode() {
        return xNode;
    }
}
