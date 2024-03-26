package csx55.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public enum UserCommands {
    LIST_PEERS("peer-nodes", 1, Collections.singletonList(NodeType.DISCOVERY)),




    PRINT_NEIGHBORS("neighbors", 2, Collections.singletonList(NodeType.PEER)),
    PRINT_FILES("files", 3, Collections.singletonList(NodeType.PEER)),

    FINGER_TABLE("finger-table", 4, Collections.singletonList(NodeType.PEER)),

    UPLOAD_FILE("upload", 11, Collections.singletonList(NodeType.PEER)),

    DOWNLOAD_FILE("download", 12, Collections.singletonList(NodeType.PEER)),


    EXIT("exit", -1, Arrays.asList(NodeType.DISCOVERY, NodeType.PEER));
    private final String cmd;
    private final int cmdId;
    private final List <NodeType> nodeType;
    private UserCommands(String cmd, int cmdId, List <NodeType> nodeType) {
        this.cmd = cmd;
        this.cmdId = cmdId;
        this.nodeType = nodeType;
    }

    public String getCmd() {
        return cmd;
    }

    public int getCmdId() {
        return cmdId;
    }

    public List<NodeType> getNodeType() {
        return nodeType;
    }

    public static List<String> getUserRegistryCommands() {
        List <String> cmdGuide = new ArrayList<>();
        for (UserCommands cmd: Arrays.stream(UserCommands.values()).filter(i -> i.getNodeType().contains(NodeType.DISCOVERY)).collect(Collectors.toList())){
            cmdGuide.add("Command : " + cmd.getCmd() +" "+ " ID: "+ cmd.getCmdId());
        }
        return cmdGuide;
    }

    public static String userRegistryCommandsToString() {
        List<String> cmdList = getUserRegistryCommands();
        return String.join("\n", cmdList);
    }

    public static List<String> getMessageNodeCommands() {
        List <String> cmdGuide = new ArrayList<>();
        for (UserCommands cmd: Arrays.stream(UserCommands.values()).filter(i -> i.getNodeType().contains(NodeType.PEER)).collect(Collectors.toList())){
            cmdGuide.add("Command : " + cmd.getCmd() +" "+ " ID: "+ cmd.getCmdId());
        }
        return cmdGuide;
    }

    public static String messageNodeCommandsToString() {
        List<String> cmdList = getMessageNodeCommands();
        return String.join("\n", cmdList);
    }



}
