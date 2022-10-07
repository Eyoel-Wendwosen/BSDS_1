package utils;

import java.util.concurrent.BlockingQueue;

public class RandomSkiDataProducer implements Runnable{
    private final int TOTAL_NUMBER_OF_REQUEST;
    private final BlockingQueue<SkiEvent> queue;
    private final Status status;

    public RandomSkiDataProducer(BlockingQueue<SkiEvent> queue, Status status, int totalRequest) {
        this.TOTAL_NUMBER_OF_REQUEST = totalRequest;
        this.queue = queue;
        this.status = status;
    }

    @Override
    public void run() {
        for (int i = 0; i < TOTAL_NUMBER_OF_REQUEST; i++) {
            SkiEvent event = RandomDataGenerator.generateRandomData();
            try {
                queue.put(event);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        this.status.setRandomGeneratorDone(true);
    }
}
