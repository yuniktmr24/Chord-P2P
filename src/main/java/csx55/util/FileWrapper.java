package csx55.util;

import java.io.Serializable;

public class FileWrapper implements Serializable {
    private static final long serialversionUID = 1L;
    private byte [] fileBytes;
    private String fileName;

    public FileWrapper(byte[] fileBytes, String fileName) {
        this.fileBytes = fileBytes;
        this.fileName = fileName;
    }

    public byte[] getFileBytes() {
        return fileBytes;
    }

    public void setFileBytes(byte[] fileBytes) {
        this.fileBytes = fileBytes;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
