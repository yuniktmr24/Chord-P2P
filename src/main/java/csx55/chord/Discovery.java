package csx55.chord;

import csx55.domain.*;
import csx55.transport.TCPConnection;
import csx55.transport.TCPServerThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Discovery extends Node implements Serializable {
    private static final Logger logger = Logger.getLogger(Discovery.class.getName());
    private static final Discovery instance = new Discovery();
    private static TCPServerThread discoveryServerThread;

    public static Discovery getInstance() {
        return instance;
    }
    public static void main(String[] args) {
        int registryPort = args.length >= 1 ? Integer.parseInt(args[0]) : 12341;
        try (ServerSocket serverSocket = new ServerSocket(registryPort)) {
            System.out.println("Server listening on port " + registryPort + "...");
            Discovery discovery = Discovery.getInstance();
            (new Thread(discoveryServerThread = new TCPServerThread(discovery, serverSocket))).start();
            discovery.startUserInputThread(serverSocket);
        } catch (IOException e) {
            logger.severe("Error in the serverSocket communication channel" + e);
        }
    }

    private void startUserInputThread(ServerSocket socket) {
        BufferedReader userInputReader = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.println("***************************************");
                System.out.println("[DISCOVERY] Enter your Discovery node command");
                System.out.println(UserCommands.userRegistryCommandsToString());
                String userInput = userInputReader.readLine();
                if (userInput.equals(UserCommands.EXIT.getCmd()) || userInput.equals(String.valueOf(UserCommands.EXIT.getCmdId()))) {
                    //exit everything
                    socket.close();
                    throw new RuntimeException("Server terminated");
                }
                else if (userInput.equals(UserCommands.LIST_PEERS.getCmd())
                        || userInput.equals(String.valueOf(UserCommands.LIST_PEERS.getCmdId()))) {
                    Chord.printPeersInNetwork();
                }
            }
        } catch (Exception e) {
            logger.severe("Error encountered while running user command "+ e);
            try {
                TimeUnit.SECONDS.sleep( 4 );
                //cleanup();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    public void handleJoin(Socket messageSource, ClientConnection connDetails, TCPConnection tcpConnection) {
        try {
            String additionalProcessingInfo = "";
            boolean success = connDetails.getRequestType().equals(RequestType.JOIN_CHORD)
                    ? Discovery.allowNodeToJoin(tcpConnection, connDetails)
                    : Discovery.allowNodeToLeave(tcpConnection, connDetails);
            boolean modifiedOverlay = false;
            if (connDetails.getRequestType().equals(RequestType.JOIN_CHORD)) {
                additionalProcessingInfo = "Joining Chord...";
            } else if (connDetails.getRequestType().equals(RequestType.LEAVE_CHORD)) {
                additionalProcessingInfo = "Leaving Chord...";
            }
            additionalProcessingInfo += (success ? "Successful. " :"Failed");

            ServerResponse serverResponse = new ServerResponse(connDetails.getRequestType(), success ? StatusCode.SUCCESS : StatusCode.FAILURE, additionalProcessingInfo);



        } catch (Exception e) {
            logger.severe("Error handling client in registry "+ e);
        }
    }

    public static boolean allowNodeToJoin(TCPConnection connDetails, ClientConnection conn) {
        PeerConnection peerConn = new PeerConnection(conn.getPeerNode(), connDetails);
        return Chord.insertPeer(peerConn);
    }

    public static boolean allowNodeToLeave(TCPConnection connDetails, ClientConnection conn) {
        PeerConnection peerConn = new PeerConnection(conn.getPeerNode(), connDetails);
        return Chord.removePeer(peerConn);
    }
}