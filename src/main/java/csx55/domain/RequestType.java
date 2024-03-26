package csx55.domain;

import java.io.Serializable;

public enum RequestType implements Serializable {
    JOIN_CHORD,
    LEAVE_CHORD,

    MESSAGING_NODES_LIST,

    REQUEST_TOTAL_TASK_INFO,

    PEER_MESSAGE,

    PULL_TRAFFIC_SUMMARY,

    LOAD_UPDATE,

    MESSAGE_ROUND_INITIATE
}
