package csx55.chord;

import csx55.domain.SuccessorNode;
import csx55.util.Tuple;

import java.io.Serializable;

public class FingerTableEntry implements Serializable {
    private static final long serialversionUID = 1L;
    private Integer key;

    private String successorNodeDesc;

    private int successorNodeId;

    private SuccessorNode successorNode;
    private Tuple keySpaceRange;

    private Integer index;

    public FingerTableEntry(Integer index, Integer id, String successor, Integer successorNodeId) {
        this.index = index;
        this.key = id;
        this.successorNodeDesc = successor;
        this.successorNodeId = successorNodeId;
        successorNode = new SuccessorNode(successor, successorNodeId);
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public Tuple getKeySpaceRange() {
        return keySpaceRange;
    }

    public void setKeySpaceRange(Tuple range) {
        this.keySpaceRange = range;
    }

    public SuccessorNode getSuccessorNode() {
        return successorNode;
    }

    public void setSuccessorNode(SuccessorNode successorNode) {
        this.successorNode = successorNode;
        setSuccessorNodeDesc(successorNode.getDescriptor());
        setSuccessorNodeId(successorNode.getPeerId());
    }

    public Integer getIndex() {
        return index;
    }

    public String getSuccessorNodeDesc() {
        return successorNodeDesc;
    }

    public void setSuccessorNodeDesc(String successorNodeDesc) {
        this.successorNodeDesc = successorNodeDesc;
    }

    public int getSuccessorNodeId() {
        return successorNodeId;
    }

    public void setSuccessorNodeId(int successorNodeId) {
        this.successorNodeId = successorNodeId;
    }
}
