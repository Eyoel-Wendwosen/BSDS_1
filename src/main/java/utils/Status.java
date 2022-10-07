package utils;

import java.util.concurrent.atomic.AtomicInteger;

public class Status {
    private boolean isRandomGeneratorDone;
    private final Object isDoneLock;
    private final AtomicInteger failedRequests;
    private final Object isFirstThreadDoneLock;
    private boolean isFirstThreadDone;

    public Status() {
        this.isRandomGeneratorDone = false;
        this.isDoneLock = new Object();
        this.isFirstThreadDoneLock = new Object();
        this.failedRequests = new AtomicInteger(0);
    }


    public boolean isRandomGeneratorDone() {
        synchronized (isDoneLock) {
            return this.isRandomGeneratorDone;
        }
    }

    public void setRandomGeneratorDone(boolean randomGeneratorDone) {
        synchronized (isDoneLock) {
            this.isRandomGeneratorDone = randomGeneratorDone;
        }
    }

    public void setIsFirstThreadDone(boolean isFirstThreadDone) {
        synchronized (isFirstThreadDoneLock) {
            this.isFirstThreadDone = isFirstThreadDone;
        }
    }

    public boolean isFirstThreadDone() {
        synchronized (isFirstThreadDoneLock) {
            return isFirstThreadDone;
        }
    }

    public AtomicInteger getFailedRequests() {
        return this.failedRequests;
    }

    public void incrementFailedRequest() {
        this.failedRequests.incrementAndGet();
    }
}
