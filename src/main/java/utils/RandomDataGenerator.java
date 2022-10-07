package utils;

public class RandomDataGenerator {
    private static final int SKIER_ID_START = 1;
    private static final int SKIER_ID_END = 100_000;
    private static final int RESORT_ID_START = 1;
    private static final int RESORT_ID_END = 10;
    private static final int LIFT_ID_START = 1;
    private static final int LIFT_ID_END = 40;
    private static final int TIME_START = 1;
    private static final int TIME_END = 360;

    public static SkiEvent generateRandomData() {
        int skierId = generateRandomNumber(SKIER_ID_START, SKIER_ID_END);
        int resortId = generateRandomNumber(RESORT_ID_START, RESORT_ID_END);
        int liftId = generateRandomNumber(LIFT_ID_START, LIFT_ID_END);
        int time = generateRandomNumber(TIME_START, TIME_END);
        SkiEvent event = new SkiEvent(skierId, resortId, liftId, time);
        event.setSeasonId(2022);
        event.setDayId(1);
        return event;
    }

    private static int generateRandomNumber(int start, int end) {
        return start + (int) Math.floor(Math.random() * (end - start));
    }


}