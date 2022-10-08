package utils;

public enum RequestType {
    GET("GET"),
    POST("POST"),
    PUT("PUT"),
    DELETE("DELETE");

    private final String type;

    RequestType(String type) {
        this.type = type;
    }
}
