package csx55.util;

import csx55.domain.ServerResponse;

import java.io.Serializable;

public class Tuple implements Serializable {
    private static final long serialversionUID = 1L;
    //all inclusive
    private int start;
    private int end;

    public Tuple(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public boolean between(int comparing) {
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
