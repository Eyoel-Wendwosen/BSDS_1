package client2;

import utils.RequestType;

import java.util.List;

public class Report {
    private long startTime;
    private long endTime;
    private int latency;
    private RequestType requestType;
    private final int statusCode;

    public Report(long startTime, long endTime, int latency, RequestType requestType, int statusCode) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.latency = latency;
        this.requestType = requestType;
        this.statusCode = statusCode;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getLatency() {
        return latency;
    }

    public void setLatency(int latency) {
        this.latency = latency;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(RequestType requestType) {
        this.requestType = requestType;
    }

    public List<String> getList() {
        return List.of(String.valueOf(startTime), String.valueOf(endTime), String.valueOf(latency), requestType.toString(), String.valueOf(statusCode));
    }
}
