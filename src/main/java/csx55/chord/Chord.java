package csx55.chord;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

//actual overlay
public class Chord {
    private static final Chord instance = new Chord();

    public static Chord getInstance() {
        return instance;
    }
    private int numberOfPeers; //to get k value = log(numberOfPeers)

    private int kValue;

    private static List<PeerConnection> peerList = Collections.synchronizedList(new ArrayList<>());

    public static synchronized boolean insertPeer(PeerConnection peerConn) {
        boolean success = false;
        try {
            success = peerList.add(peerConn);
            if (success) {
                success = adjustFingerTable();
            }
            return success;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static synchronized boolean removePeer(PeerConnection peerConn) {
        try {
            return peerList.removeIf(p -> p.getPeer().getPeerDescriptor().equals(peerConn.getPeer().getPeerDescriptor()));
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    //adjust FT when nodes leave or join
    public static boolean adjustFingerTable () {
        try {
            return true;
        }
        catch (Exception ex) {
            return false;
        }
    }

    public static void printPeersInNetwork() {
        peerList.stream().sorted(Comparator.comparing(p -> p.getPeer().getPeerId())).forEach(p -> {
            System.out.println(p.getPeer().getPeerDescriptor());
        });
    }


    //migrate files to new nodes, as a result of adjustment
    private void migrateStoredFiles () {}

    private int getNumberOfPeers() {
        return numberOfPeers;
    }

    private void setNumberOfPeers() {
        this.numberOfPeers = peerList.size();
    }

    private void setkValue() {
        this.kValue = (int) Math.log(getNumberOfPeers());
    }

    private int getKValue() {
        return this.kValue;
    }
}
