package csx55.domain;

import java.io.Serializable;

public class UpdatePredecessorPayload implements Serializable {
        private static final long serialversionUID = 1L;
        public ChordNode xNode;

        public UpdatePredecessorPayload(ChordNode xNode) {
                this.xNode = xNode;
            }

        public ChordNode getxNode() {
            return xNode;
        }
}
