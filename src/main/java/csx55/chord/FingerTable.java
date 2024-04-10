package csx55.chord;

import csx55.config.ChordConfig;
import csx55.domain.ChordNode;
import csx55.domain.PredecessorNode;
import csx55.domain.SuccessorNode;
import csx55.util.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FingerTable implements Serializable {
    private static final long serialversionUID = 1L;

    private SuccessorNode successorNode;

    private PredecessorNode predecessorNode;

    private ChordNode currentNode;


    private List<FingerTableEntry> ftEntries = new ArrayList<>();

    private int numEntries;

    //32 bit, 64 bit ID space etc
    public FingerTable(long nodeId, String descriptor) {
        currentNode = new ChordNode(descriptor, nodeId);
    }

    public void addEntry (FingerTableEntry entry) {
        this.ftEntries.add(entry);
        if (this.ftEntries.size() == ChordConfig.NUM_PEERS) {
            computeKeySpaceRanges();
        }
    }

    public void computeKeySpaceRanges() {
        //idx = next node index
        int next = 1;
        for (FingerTableEntry ft: ftEntries) {
            if (next > ftEntries.size()) {
                break;
            }
            //at last element now
            //circle back to the first entry
            else if (next == ftEntries.size()) {
                ft.setKeySpaceRange(new Tuple(ft.getKey(), ftEntries.get(0).getKey() - 1));
            }
            else {
                ft.setKeySpaceRange(new Tuple(ft.getKey(), ftEntries.get(next).getKey() - 1));
            }
            next++;
        }
    }


    public void printFingerTable() {
        // Check if the predecessor node is not null before trying to access its properties
        if (this.getPredecessorNode() != null) {
            System.out.printf("Predecessor Node Desc: %s | Predecessor Node ID: %d \n",
                    this.getPredecessorNode().getDescriptor(),
                    this.getPredecessorNode().getPeerId());
        } else {
            System.out.println("Predecessor Node Desc: null | Predecessor Node ID: null");
        }

        if (this.getSuccessorNode() != null) {
            System.out.printf("Successor Node Desc: %s | Successor Node ID: %d \n",
                    this.getSuccessorNode().getDescriptor(),
                    this.getSuccessorNode().getPeerId());
        } else {
            System.out.println("Successor Node Desc: null | Successor Node ID: null");
        }
        System.out.println("Index \t Key_start \t\t Key_Range \t\t\t Successor Desc. \t\t Succesor ID");
        int index = 1;
        for (FingerTableEntry ft: ftEntries) {
            String formatted = String.format("%d \t %d \t %s \t %s \t %d", index,
                    ft.getKey(),
                    ft.getKeySpaceRange().toString(),
                    ft.getSuccessorNodeDesc(),
                    ft.getSuccessorNodeId());
            System.out.println(formatted);
            index++;
        }
    }

    //nodes this FT contains routing info to
    public List <String> findDistinctRoutableNodesInFT () {
        return ftEntries.stream()
                .map(FingerTableEntry::getSuccessorNodeDesc)
                .distinct()
                .collect(Collectors.toList());
    }


    public List <FingerTableEntry> getEntriesSmallerThanNewNodeKey (long nodeKey) {
        return ftEntries.stream()
                .filter(entry -> entry.getKey() < nodeKey) // Filter based on id
                .collect(Collectors.toList());
    }

    public List <FingerTableEntry> getEntriesSmallerThanEqualToNewNodeKey (long nodeKey) {
        return ftEntries.stream()
                .filter(entry -> entry.getKey() <= nodeKey) // Filter based on id
                .collect(Collectors.toList());
    }

    public FingerTableEntry getEntryForWhichKeyIsInRange (int nodeKey) {
        Optional<FingerTableEntry> entryOptional = ftEntries.stream()
                .filter(entry -> entry.getKeySpaceRange().between(nodeKey))
                .findFirst();
        return entryOptional.get();
    }

    public FingerTableEntry lookup(long k) {
        return ftEntries.stream()
                .filter(entry -> entry.getKey() >= k)
                .min(Comparator.comparingLong(FingerTableEntry::getKey))
                .orElseGet(() ->
                        // If no entry is found that is greater than or equal to k,
                        // return the entry with the highest key.
                        ftEntries.stream()
                                .max(Comparator.comparingLong(FingerTableEntry::getKey))
                                .orElseThrow()
                );
    }

    //TODO: circularLookup?
    public FingerTableEntry circularLookup(long k, long xNode) {
        return ftEntries.stream()
                .filter(entry -> entry.getKey() >= k
                        ||
                        entry.getKey() >= 0 && entry.getKey() <= xNode)
                .min(Comparator.comparingLong(FingerTableEntry::getKey))
                .orElseGet(() ->
                        // If no entry is found that is greater than or equal to k,
                        // return the entry with the highest key.
                        ftEntries.stream()
                                .max(Comparator.comparingLong(FingerTableEntry::getKey))
                                .orElseThrow()
                );
    }

    //xNode = newly Inserted node
    public List <FingerTableEntry> getToBeModifiedFingersCircular(long current, long xNode) {
        return ftEntries.stream()
                .filter(entry -> (entry.getKey() > current ||
                        entry.getKey() >= 0 && entry.getKey() <= xNode))
                .collect(Collectors.toList());
    }

    public List<FingerTableEntry> getFtEntries() {
        return ftEntries;
    }

    public void setFtEntries(List<FingerTableEntry> ftEntries) {
        this.ftEntries = ftEntries;
    }

    public int getNumEntries() {
        return numEntries;
    }

    public void setNumEntries(int numEntries) {
        this.numEntries = numEntries;
    }

    public SuccessorNode getSuccessorNode() {
        return successorNode;
    }

    public void setSuccessorNode(SuccessorNode successorNode) {
        this.successorNode = successorNode;
    }

    public PredecessorNode getPredecessorNode() {
        return predecessorNode;
    }

    public void setPredecessorNode(PredecessorNode predecessorNode) {
        this.predecessorNode = predecessorNode;
    }

    public ChordNode getCurrentNode() {
        return currentNode;
    }

    public void setCurrentNode(ChordNode currentNode) {
        this.currentNode = currentNode;
    }

    public ChordNode findClosestPrecedingNode (long newnodeId) {
        for (int i = 32; i <= 1; i--) {
            if (nodeBetween(ftEntries.get(i).getSuccessorNodeId(), this.currentNode.getPeerId(), this.successorNode.getPeerId())) {
                return ftEntries.get(i).getSuccessorNode();
            }
        }
        //so that key = 8 lookup at node = 8 returns 8 and doesn't run into stack overflow
        if (newnodeId == this.currentNode.getPeerId()) {
            return this.currentNode;
        }
        if (newnodeId < this.currentNode.getPeerId()) {
            return this.predecessorNode;
        }
        return this.successorNode;
        //return this.currentNode;
    }

    private boolean nodeBetween (long newnodeId, long bootstrapnodeId, long successornodeId) {
        if (bootstrapnodeId < successornodeId) {
            return bootstrapnodeId <= newnodeId && newnodeId <= successornodeId;
        }
        else {
            return newnodeId > bootstrapnodeId || newnodeId < successornodeId;
        }
    }
}
