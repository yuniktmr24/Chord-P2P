package csx55.util;

import csx55.domain.ServerResponse;

import java.io.Serializable;

public class Tuple implements Serializable {
    private static final long serialversionUID = 1L;
    //all inclusive
    private long start;
    private long end;

    public Tuple(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public boolean between(long comparing) {
        if (comparing >= start && comparing <= end) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "["+ start + "," + end + "]";
    }
}
