package utils;

import org.apache.http.client.HttpRequestRetryHandler;

public class NetworkHandler {
    public static HttpRequestRetryHandler retryHandler() {
        return (exception, executionCount, context) -> {

            System.out.println("try request: " + executionCount);

            // Do not retry if over max retry count
            return executionCount < 5;
        };
    }
}
