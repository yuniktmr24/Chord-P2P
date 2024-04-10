package csx55.util;

import csx55.chord.FingerTableEntry;
import csx55.config.ChordConfig;
import csx55.domain.*;
import csx55.transport.TCPConnection;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class PeerFingerTableUtility {

    /** scrap code storage
    /***
     * Ask bootstrap node to check for successor
     */
/*** Scrap code storage
    private SuccessorNode findSuccessor(int targetKey, String bootstrapNodeIp, int bootstrapNodePort) throws InterruptedException {
        //first get FT from bootstrapNode;
        if (boostrapNodeFingerTable == null)
        {
            TCPConnection conn = getTCPConnection(tcpCache, bootstrapNodeIp, bootstrapNodePort);
            tcpCache.put(bootstrapNodeIp + ":" + bootstrapNodePort, conn);
            requestFingerTableFromBootstrapNode(conn);
        }
        //might need to add the last node in circle check
        boolean boostrapNodeIsLastNode = false;
        int bootstrapNodeId = boostrapNodeFingerTable.getCurrentNode().getPeerId();
        int successorNodeId = boostrapNodeFingerTable.getSuccessorNode() == null ?
                bootstrapNodeId :
                boostrapNodeFingerTable.getSuccessorNode().getPeerId();
        //second node in network case
        if (boostrapNodeFingerTable.getSuccessorNode() == null) {
            //refresh FT from boostrap node aka adam node
            ackLatch = new CountDownLatch(1);
            forceSuccessorUpdateIfNull();
            ackLatch.await();
            requestFingerTableFromBootstrapNode(getTCPConnection(tcpCache, bootstrapNodeIp, bootstrapNodePort));

            this.successorNode = new SuccessorNode(boostrapNodeFingerTable.getCurrentNode().getDescriptor(),
                    boostrapNodeFingerTable.getCurrentNode().getPeerId());
            this.predecessorNode = new PredecessorNode(boostrapNodeFingerTable.getCurrentNode().getDescriptor(),
                    boostrapNodeFingerTable.getCurrentNode().getPeerId());
            return (SuccessorNode) this.successorNode;
        }
        if (bootstrapNodeId > successorNodeId) {
            boostrapNodeIsLastNode = true;
        }
        boolean inBetweenTerminalBootstrapNodeAndSuccessor =
                (boostrapNodeIsLastNode
                        && (targetKey > bootstrapNodeId))
                        ||
                        (boostrapNodeIsLastNode
                                && (targetKey < bootstrapNodeId && targetKey <= successorNodeId));

        if (inBetweenTerminalBootstrapNodeAndSuccessor ||
                (targetKey > boostrapNodeFingerTable.getCurrentNode().getPeerId()
                        && targetKey <= boostrapNodeFingerTable.getSuccessorNode().getPeerId())) {
            successorNode = new SuccessorNode(
                    boostrapNodeFingerTable.getSuccessorNode().getDescriptor(),
                    boostrapNodeFingerTable.getSuccessorNode().getPeerId());
            return boostrapNodeFingerTable.getSuccessorNode();
        }
        else {
            ChordNode closestPrecedingNode = getClosestPrecedingNode(targetKey, boostrapNodeFingerTable.getCurrentNode());
            String ip = closestPrecedingNode.getDescriptor().split(":")[0];
            int port = Integer.parseInt(
                    closestPrecedingNode.getDescriptor()
                            .split(":")[1]);

            return findSuccessor(targetKey, ip, port);
        }
    }

    private ChordNode getClosestPrecedingNode(int targetKey, ChordNode currentNode) {
        for (int idx = ChordConfig.NUM_PEERS; idx >= 1; idx--) {
            SuccessorNode currentSuccessor = boostrapNodeFingerTable.getFtEntries().get(idx - 1).getSuccessorNode();
            int currentSuccessorId = currentSuccessor.getPeerId();

            boolean isInCircularInterval = (currentNode.getPeerId() < targetKey) ?
                    (currentSuccessorId > currentNode.getPeerId() && currentSuccessorId < targetKey) : // Normal case
                    (currentSuccessorId > currentNode.getPeerId() || currentSuccessorId < targetKey); // Circular case

            if (isInCircularInterval) {
                return currentSuccessor;
            }
            //account for terminal node problem here as well TODO
            if (currentSuccessorId > currentNode.getPeerId() && currentSuccessorId < targetKey) {
                return currentSuccessor;
            }
        }
        return currentNode;
    }



 private void joinUsingBootstrapNode(Peer bootstrapNode) {
 try {
 //already established connection to node then
 if (tcpCache.containsKey(bootstrapNode.getPeerDescriptor())) {
 TCPConnection connection = tcpCache.get(bootstrapNode.getPeerDescriptor());

 //TODO: now start creating the FT
 //initFingerTable();
 requestFingerTableFromBootstrapNode(connection);
 }
 else {
 Socket bootstrapNodeSocket = new Socket(bootstrapNode.getNodeIp(), bootstrapNode.getNodePort());
 TCPConnection connection = new TCPConnection(this, bootstrapNodeSocket);
 connection.startConnection();

 tcpCache.put(bootstrapNode.getPeerDescriptor(), connection);

 //TODO COMPLETE: now start creating the FT
 requestFingerTableFromBootstrapNode(connection);
 adjustFingerTableUsingBootstrapNode();
 }
 } catch (IOException e) {
 throw new RuntimeException(e);
 }
 }


 //IMPORTANT: 1-Based indexing scheme for FT
 private void adjustFingerTableUsingBootstrapNode() {
 List <String> distinctRoutes = this.boostrapNodeFingerTable.findDistinctRoutableNodesInFT();
 if (distinctRoutes.size() == 1) {
 System.out.println("The Boostrap FT is at the End of Ring, thus routes to a single node");
 //more logic
 List <FingerTableEntry> candidateStorageEntries = boostrapNodeFingerTable.getEntriesSmallerThanNewNodeKey(this.getPeerId());

 //update FT in the predecessor node
 //migrate keys to the new node


 }
 else {

 }
 }
    */

//can't have null successors if more than 2 nodes in the overlay
/*
private void forceSuccessorUpdateIfNull() {
    String ip = boostrapNodeFingerTable.getCurrentNode().getDescriptor()
            .split(":")[0];
    int port = Integer.parseInt(boostrapNodeFingerTable.getCurrentNode().getDescriptor()
            .split(":")[1]);

    TCPConnection conn = getTCPConnection(tcpCache,ip,port);

    Message msg = new Message(Protocol.SEND_SUCCESSOR_INFO, this.getPeerDescriptor() + ","  + this.getPeerId());
    try {
        conn.getSenderThread().sendData(msg);
    } catch (InterruptedException e) {
        throw new RuntimeException(e);
    }
}

    public void ackSuccessorUpdateBecauseNull(TCPConnection conn, Message msg) {
        System.out.println("Received successor info");
        String newNodeDesc = msg.getPayload().toString().split(",")[0];
        String newNodeId = msg.getPayload().toString().split(",")[1];
//        //add new node to successor chain
        if (predecessorNode == null && successorNode == null) {
            //typically when two nodes in ring and we callback to the first node
            successorNode = new SuccessorNode(newNodeDesc, Integer.parseInt(newNodeId));
            predecessorNode = new PredecessorNode(newNodeDesc, Integer.parseInt(newNodeId));
            this.fingerTable.setSuccessorNode((SuccessorNode) successorNode);
            this.fingerTable.setPredecessorNode((PredecessorNode) predecessorNode);
        }
        //update FT while you're at it (???)
        List<FingerTableEntry> smallerEntries = this.fingerTable.
                getEntriesSmallerThanNewNodeKey(Integer.parseInt(newNodeId));

        int smallerEntriesIdx = 0;
        int lastKeyIsResponsibleForIndex = 0;
        for (FingerTableEntry entry: smallerEntries) {
            entry.setSuccessorNode((SuccessorNode) successorNode);
            smallerEntriesIdx++;
            //update the key space in largest finger smaller than the new key
            if (smallerEntriesIdx == smallerEntries.size() - 1) {
                entry.setKeySpaceRange(new Tuple(entry.getKey(),
                        successorNode.getPeerId()));
                entry.setSuccessorNode((SuccessorNode) successorNode);
                lastKeyIsResponsibleForIndex = entry.getIndex();
            }
        }
        FingerTableEntry nextEntryBeyondNode = this.fingerTable.getFtEntries()
                .get((lastKeyIsResponsibleForIndex + 1) % ChordConfig.NUM_PEERS);
        Tuple oldKeySpace = nextEntryBeyondNode.getKeySpaceRange();
        nextEntryBeyondNode.setKeySpaceRange(new Tuple(successorNode.getPeerId() + 1,
                oldKeySpace.getEnd()));
        //send updated finger table over the paired node
        //sendFingerTable(conn);
        Message ack = new Message(Protocol.ACK, "");
        try {
            conn.getSenderThread().sendData(ack);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    */
}

