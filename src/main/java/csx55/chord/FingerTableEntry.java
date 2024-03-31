package csx55.chord;

import csx55.util.Tuple;

import java.io.Serializable;

public class FingerTableEntry implements Serializable {
    private static final long serialversionUID = 1L;
    private Integer key;

    private String successorNode;

    private Tuple keySpaceRange;

    public FingerTableEntry(Integer id, String nodeResponsible) {
        this.key = id;
        this.successorNode = nodeResponsible;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public String getSuccessorNode() {
        return successorNode;
    }

    public void setSuccessorNode(String successorNode) {
        this.successorNode = successorNode;
    }

    public Tuple getKeySpaceRange() {
        return keySpaceRange;
    }

    public void setKeySpaceRange(Tuple range) {
        this.keySpaceRange = range;
    }
}
