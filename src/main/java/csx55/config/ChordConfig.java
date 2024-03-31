package csx55.config;


import csx55.chord.FingerTable;
import csx55.chord.PeerConnection;

import java.util.*;
import java.util.stream.Collectors;

//actual overlay
public class ChordConfig {

    public static final int NUM_PEERS = 32; //to get k value = log(numberOfPeers)

    public static final int STABLIZATION_INTERVAL = 1000;

    private int kValue;

    //change this to bool -> string. return random live peer's network info
//    public static synchronized boolean insertPeer(PeerConnection newPeerConn) {
//        boolean success = false;
//        PeerConnection entryPeer = null;
//        try {
//            if (peerList.isEmpty()) {
//                success = peerList.add(newPeerConn);
//                adjustFingerTable(newPeerConn, null);
//            }
//            else {
//                //entryPeer -> peer that helps new node to join chord
//                entryPeer = returnRandomNodeForEntry();
//
//                FingerTable referenceFT = entryPeer.getPeer().getFingerTable();
//
//                //step 1: pick the smallest successor nodeId in the finger table greater than new
//                //peer ID. If peerId larger than the largest entry, then we'll return largest succ entry
//
//                //step2: get this successor node's FT now
//                //step 3: iterate through this node's FT
//                //repeat step1
//
//                //step 4: once successor candidate found (set succ (newKeyId) -> nodeId found via 1)
//                //step 5:
//                // this value may not be correct now
//                //but stabilizer function (configurable) will enforce correctness later
//                // and ensure pred, succ values are set and correct.
//
//                referenceFT
//            }
//            if (success) {
//                success = adjustFingerTable(newPeerConn, entryPeer);
//            }
//            return success;
//        }
//        catch (Exception ex) {
//            ex.printStackTrace();
//            return false;
//        }
//    }
}
