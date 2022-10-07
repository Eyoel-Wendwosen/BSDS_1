package client2;

import client1.Client;
import utils.RandomSkiDataProducer;
import utils.SkiEvent;
import utils.Status;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MultiThreadedClient2 {
    private static final int NUM_OF_THREAD = 32;
    private static final int BLOCKING_QUEUE_SIZE = 32;
    private static final int TOTAL_NUMBER_OF_REQUESTS = 200_000;
    private static final int SEC_TO_MIL_SEC = 1000;

    public static void main(String[] args) throws InterruptedException {

        BlockingQueue<SkiEvent> skiEventBlockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);

        ConcurrentHashMap<String, List<Report>> summaryReport = new ConcurrentHashMap<>();

        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        System.out.println("Start time: " + startTime.getTime());

        Status status = new Status();

        Thread dataProducer = new Thread(new RandomSkiDataProducer(skiEventBlockingQueue, status, TOTAL_NUMBER_OF_REQUESTS));
        dataProducer.start();

        for (int i = 0; i < NUM_OF_THREAD; i++) {
            Thread clientThread = new Thread(new Client2(skiEventBlockingQueue, status, true, summaryReport));
            clientThread.start();
        }

        Timestamp t1 = new Timestamp(System.currentTimeMillis());
        System.out.println("Started first 32 Threads: " + t1.getTime());
        int count = 0;
        while (!status.isFirstThreadDone()) {
            if (count < 1) {
                System.out.println("Waiting for the 1st thread to finish. ");
                count++;
            }
        }

        int numThread = NUM_OF_THREAD * 6;
        Thread[] clientThreads = new Thread[numThread];

        for (int i = 0; i < numThread; i++) {
            Thread clientThread = new Thread(new Client2(skiEventBlockingQueue, status, false, summaryReport));
            clientThreads[i] = clientThread;
            clientThread.start();
        }

        for (int i = 0; i < numThread; i++) {
            clientThreads[i].join();
        }

        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        System.out.println("End time: " + endTime.getTime());

        long wallTime = endTime.getTime() - startTime.getTime();

        System.out.println("----------------------------------");
        System.out.println("Time it took for 200k requests: " + wallTime);
        System.out.println("Failed requests: " + status.getFailedRequests().get());
        System.out.println("Requests Per Second(Throughput): " + (TOTAL_NUMBER_OF_REQUESTS / (wallTime / SEC_TO_MIL_SEC)));

        ReportGenerator reportGenerator = new ReportGenerator(summaryReport);
        reportGenerator.generateReport();
        reportGenerator.writeToCSV();


    }
}
