package utils;

import java.util.HashMap;
import java.util.Map;

public class SkiEvent {
    private int skierId;
    private int resortId;
    private int liftId;
    private int seasonId;
    private int dayId;
    private int time;

    public SkiEvent() {
    }

    public SkiEvent(int skierId, int resortId, int liftId, int time) {
        this.skierId = skierId;
        this.resortId = resortId;
        this.liftId = liftId;
        this.time = time;
    }

    public int getSkierId() {
        return skierId;
    }

    public void setSkierId(int skierId) {
        this.skierId = skierId;
    }

    public int getResortId() {
        return resortId;
    }

    public void setResortId(int resortId) {
        this.resortId = resortId;
    }

    public int getLiftId() {
        return liftId;
    }

    public void setLiftId(int liftId) {
        this.liftId = liftId;
    }

    public int getSeasonId() {
        return seasonId;
    }

    public void setSeasonId(int seasonId) {
        this.seasonId = seasonId;
    }

    public int getDayId() {
        return dayId;
    }

    public void setDayId(int dayId) {
        this.dayId = dayId;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public Map<String, Integer> getRequestBody() {
        Map<String, Integer> body = new HashMap<>();
        body.put("liftId", getLiftId());
        body.put("time", getTime());
        return body;
    }
}
