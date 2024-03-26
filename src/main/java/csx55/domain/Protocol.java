package csx55.domain;

public interface Protocol {
    final int JOIN_CHORD_REQUEST = 1;
    final int JOIN_CHORD_RESPONSE = 2;
    final int RANDOM_PEER_DISCOVERY = 3;
    final int LEAVE_CHORD = 4;
    final int ADJUST_FINGER_TABLE_DONE = 5;

    final int CLIENT_CONNECTION = 6;

    final int SERVER_RESPONSE = 7;
}
