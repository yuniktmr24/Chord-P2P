package csx55.domain;

import java.io.*;

public class ServerResponse implements Serializable{
    private final int type = Protocol.SERVER_RESPONSE;
    public RequestType requestType;
    public StatusCode statusCode;
    public String additionalInfo;

    public ServerResponse(RequestType requestType, StatusCode statusCode, String additionalInfo) {
        this.requestType = requestType;
        this.statusCode = statusCode;
        this.additionalInfo = additionalInfo;
    }

    public ServerResponse() {

    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(StatusCode statusCode) {
        this.statusCode = statusCode;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    @Override
    public String toString() {
        return "ServerResponse{" +
                "requestType=" + requestType +
                ", statusCode=" + statusCode +
                ", additionalInfo='" + additionalInfo + '\'' +
                '}';
    }
    public int getType() {
        return type;
    }
}
