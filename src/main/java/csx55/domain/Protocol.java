package csx55.domain;

import java.io.Serializable;

public interface Protocol {
    final int JOIN_CHORD_REQUEST = 1;
    final int JOIN_CHORD_RESPONSE = 2;
    final int RANDOM_PEER_DISCOVERY = 3;
    final int LEAVE_CHORD = 4;
    final int ADJUST_FINGER_TABLE_DONE = 5;

    final int CLIENT_CONNECTION = 6;

    final int SERVER_RESPONSE = 7;

    final int NEW_PEER_ID = 8;

    final int BOOSTRAPPING_NODE_INFO = 9;

    final int ADAM_NODE_INF0 = 10;

    final int REQUEST_FINGER_TABLE = 11;

    final int SEND_FINGER_TABLE = 12;

    final int SEND_SUCCESSOR_INFO = 13;

    final int ACK = 14;

    final int STABILIZE = 15;
}

