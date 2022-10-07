package client2;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import utils.NetworkHandler;
import utils.SkiEvent;
import utils.Status;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Client2 implements Runnable {
    private final int TOTAL_NUM_OF_REQUEST = 1000;
    private static final String localURL = "http://localhost:8080/";
    private static final String EC2URL = "http://ec2-52-25-141-133.us-west-2.compute.amazonaws.com:8080/";
    private static final String APP_NAME = "Java_Servlet_war_exploded";
    private static final String EC2_APP_NAME = "Java_Servlet_war";
    private final Object synchronizationObject;
    private final HttpClient client;
    private final BlockingQueue<SkiEvent> queue;
    private final Status status;
    private final boolean isFirst32;
    private final ConcurrentHashMap<String, List<Report>> reportMap;


    public Client2(BlockingQueue<SkiEvent> queue, Status status, boolean isFirst32, ConcurrentHashMap<String, List<Report>> concurrentHashMap) {
        this.isFirst32 = isFirst32;
        this.queue = queue;
        this.synchronizationObject = new Object();
        this.status = status;
        this.reportMap = concurrentHashMap;
        this.client = HttpClients.custom().addInterceptorLast(new HttpResponseInterceptor() {
            @Override
            public void process(HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException {
                if ((double) httpResponse.getStatusLine().getStatusCode() / 400.0 > 1.0) {
                    throw new IOException("Retrying now");
                }
            }
        }).setRetryHandler(NetworkHandler.retryHandler()).build();
    }

    @Override
    public void run() {
        this.reportMap.put(Thread.currentThread().getName(), new ArrayList<>());
        if (isFirst32) {
            for (int i = 0; i < TOTAL_NUM_OF_REQUEST; i++) {
                this.sendRequest();
            }

            synchronized (this.synchronizationObject) {
                if (!this.status.isFirstThreadDone()) {
                    System.out.println("First thread to finish " + Thread.currentThread().getName());
                    this.status.setIsFirstThreadDone(true);
                }
            }

        } else {
            while (!this.status.isRandomGeneratorDone()) {
                sendRequest();
            }
        }
    }

    private void sendRequest() {
        HttpPost postMethod = new HttpPost(EC2URL);
        SkiEvent randomSkiEvent = null;
        try {
            randomSkiEvent = queue.poll(100, TimeUnit.MILLISECONDS);
            if (randomSkiEvent == null) return;

            URIBuilder uri = new URIBuilder(EC2URL);
            uri.setPath(String.format("%s/skiers/%d/seasons/%d/days/%d/skiers/%d", EC2_APP_NAME, randomSkiEvent.getResortId(), randomSkiEvent.getSeasonId(), randomSkiEvent.getDayId(), randomSkiEvent.getSkierId()));
            postMethod.setURI(uri.build());


            postMethod.setEntity(new StringEntity(randomSkiEvent.getRequestBody().toString(), ContentType.APPLICATION_JSON));

            Timestamp arrivalTime = new Timestamp(System.currentTimeMillis());

            HttpResponse httpResponse = this.client.execute(postMethod);
            int statusCode = httpResponse.getStatusLine().getStatusCode();

            if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_CREATED) {
                System.err.println("Method Failed: " + statusCode + EntityUtils.toString(httpResponse.getEntity()));
            }

            Timestamp responseTime = new Timestamp(System.currentTimeMillis());

            Report report = new Report(arrivalTime.getTime(), responseTime.getTime(),
                    (int) (responseTime.getTime() - arrivalTime.getTime()), RequestType.POST, statusCode);

            reportMap.get(Thread.currentThread().getName()).add(report);

//            String responseBody = EntityUtils.toString(httpResponse.getEntity());
//            System.out.println("Response: " + responseBody);
//
        } catch (URISyntaxException | IOException e) {
            this.status.incrementFailedRequest();
        } catch (InterruptedException e) {
            System.out.println("Blockingqueue empty");
        } finally {
            postMethod.releaseConnection();
        }
    }
}
