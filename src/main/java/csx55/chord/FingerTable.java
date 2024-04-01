package csx55.chord;

import csx55.config.ChordConfig;
import csx55.util.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FingerTable implements Serializable {
    private static final long serialversionUID = 1L;
    private String predecessorNodeDesc;

    private String successorNodeDesc;

    private List<FingerTableEntry> ftEntries = new ArrayList<>();

    private int numEntries;

    //32 bit, 64 bit ID space etc
    public FingerTable() {
    }

    public void addEntry (FingerTableEntry entry) {
        this.ftEntries.add(entry);
        if (this.ftEntries.size() == ChordConfig.NUM_PEERS) {
            computeKeySpaceRanges();
        }
    }

    private void computeKeySpaceRanges() {
        //idx = next node index
        int next = 1;
        for (FingerTableEntry ft: ftEntries) {
            if (next > ftEntries.size()) {
                break;
            }
            //at last element now
            //circle back to the first entry
            else if (next == ftEntries.size()) {
//                ft.setKeySpaceRange(
//                        ft.getKey()
//                                + ":"
//                                + (ftEntries.get(0).getKey() - 1)
//                );
                ft.setKeySpaceRange(new Tuple(ft.getKey(), ftEntries.get(0).getKey() - 1));
            }
            else {
//                ft.setKeySpaceRange(
//                        ft.getKey()
//                                + ":"
//                                + (ftEntries.get(next).getKey() - 1)
//                );
                ft.setKeySpaceRange(new Tuple(ft.getKey(), ftEntries.get(next).getKey() - 1));
            }
            next++;
        }
    }


    public void printFingerTable() {
        System.out.println("Index \t\t Key_start \t\t Key_Range \t\t Successor Desc.");
        int index = 1;
        for (FingerTableEntry ft: ftEntries) {
            String formatted = String.format("%d \t %d \t %s \t %s", index,
                    ft.getKey(),
                    ft.getKeySpaceRange().toString(),
                    ft.getSuccessorNode());
            System.out.println(formatted);
            index++;
        }
    }

    //nodes this FT contains routing info to
    public List <String> findDistinctRoutableNodesInFT () {
        return ftEntries.stream()
                .map(FingerTableEntry::getSuccessorNode)
                .distinct()
                .collect(Collectors.toList());
    }

    public String getPredecessorNodeDesc() {
        return predecessorNodeDesc;
    }

    public void setPredecessorNodeDesc(String predecessorNodeDesc) {
        this.predecessorNodeDesc = predecessorNodeDesc;
    }

    public String getSuccessorNodeDesc() {
        return successorNodeDesc;
    }

    public void setSuccessorNodeDesc(String successorNodeDesc) {
        this.successorNodeDesc = successorNodeDesc;
    }

    public List <FingerTableEntry> getEntriesSmallerThanNewNodeKey (int nodeKey) {
        return ftEntries.stream()
                .filter(entry -> entry.getKey() < nodeKey) // Filter based on id
                .collect(Collectors.toList());
    }


    public FingerTableEntry getEntryForWhichKeyIsInRange (int nodeKey) {
        Optional<FingerTableEntry> entryOptional = ftEntries.stream()
                .filter(entry -> entry.getKeySpaceRange().between(nodeKey))
                .findFirst();
        return entryOptional.get();
    }

    public FingerTableEntry lookup(Integer k) {
        return ftEntries.stream()
                .filter(entry -> entry.getKey() >= k)
                .min(Comparator.comparingInt(FingerTableEntry::getKey))
                .orElseGet(() ->
                        // If no entry is found that is greater than or equal to k,
                        // return the entry with the highest key.
                        ftEntries.stream()
                                .max(Comparator.comparingInt(FingerTableEntry::getKey))
                                .orElseThrow()
                );
    }
}
