import client1.Client;
import client2.Client2;
import client2.Report;
import client2.ReportGenerator;
import utils.RandomSkiDataProducer;
import utils.SkiEvent;
import utils.Status;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MultiThreadedClient {
    private static final int NUM_OF_THREAD = 32;
    private static final int BLOCKING_QUEUE_SIZE = 32;
    private static final int TOTAL_NUMBER_OF_REQUESTS = 200_000;
    private static final int SEC_TO_MIL_SEC = 1000;
    private static String uri;
    private static String appName;
    private static String port;
    private static int clientType;


    public static void main(String[] args) throws InterruptedException {

        if (args.length == 3) {
            System.out.printf("Three arguments were required but only %d give.%n", args.length);
            return;
        }
        try {
            uri = args[0];
            port = args[1];
            appName = args[2];
            clientType = Integer.parseInt(args[3]);

        } catch (Exception e) {
            System.out.println("Some error with the parameters " + e.getMessage());
            System.exit(0);
        }

        BlockingQueue<SkiEvent> skiEventBlockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);

        ConcurrentHashMap<String, List<Report>> summaryReport = new ConcurrentHashMap<>();

        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        System.out.printf("Start time(32-threads): %d ms%n", startTime.getTime());

        Status status = new Status();

        Thread dataProducer = new Thread(new RandomSkiDataProducer(skiEventBlockingQueue, status, TOTAL_NUMBER_OF_REQUESTS));
        dataProducer.start();

        if (clientType == 0) {
            for (int i = 0; i < NUM_OF_THREAD; i++) {
                Thread clientThread = new Thread(getClient1(skiEventBlockingQueue, status, true));
                clientThread.start();
            }
        } else {
            for (int i = 0; i < NUM_OF_THREAD; i++) {
                Thread clientThread = new Thread(getClient2(skiEventBlockingQueue, status, true, summaryReport));
                clientThread.start();
            }
        }

        int count = 0;
        while (!status.isFirstThreadDone()) {
            if (count < 1) {
                System.out.println("Waiting for the 1st thread to finish. ");
                count++;
            }
        }

        int numThread = NUM_OF_THREAD * 6;
        Timestamp t1 = new Timestamp(System.currentTimeMillis());
        System.out.printf("Started other %d Threads: %d ms%n", numThread, t1.getTime());
        Thread[] clientThreads = new Thread[numThread];

        if (clientType == 0) {
            for (int i = 0; i < numThread; i++) {
                Thread clientThread = new Thread(getClient1(skiEventBlockingQueue, status, true));
                clientThreads[i] = clientThread;
                clientThread.start();
            }
        } else {
            for (int i = 0; i < numThread; i++) {
                Thread clientThread = new Thread(getClient2(skiEventBlockingQueue, status, true, summaryReport));
                clientThreads[i] = clientThread;
                clientThread.start();
            }
        }

        for (int i = 0; i < numThread; i++) {
            clientThreads[i].join();
        }

        Timestamp endTime = new Timestamp(System.currentTimeMillis());
        System.out.printf("End time: %d ms%n", endTime.getTime());

        long wallTime = endTime.getTime() - startTime.getTime();

        System.out.println("----------------------------------");
        System.out.printf("Time it took for 200k requests: %d ms%n", wallTime);
        System.out.printf("Failed requests: %d ms%n", status.getFailedRequests().get());
        System.out.println("Requests Per Second(Throughput): " + (TOTAL_NUMBER_OF_REQUESTS / (wallTime / SEC_TO_MIL_SEC)));

        if (clientType != 0) {
            ReportGenerator reportGenerator = new ReportGenerator(summaryReport);
            reportGenerator.generateReport();
            reportGenerator.writeToCSV();
        }
    }

    public static Runnable getClient1(BlockingQueue<SkiEvent> skiEventBlockingQueue, Status status, boolean isFirst32) {
        return new Client(skiEventBlockingQueue, status, isFirst32, uri, appName, port);
    }

    public static Runnable getClient2(BlockingQueue<SkiEvent> skiEventBlockingQueue, Status status, boolean isFirst32, ConcurrentHashMap<String, List<Report>> summaryReport) {
        return new Client2(skiEventBlockingQueue, status, isFirst32, uri, appName, port, summaryReport);
    }
}
