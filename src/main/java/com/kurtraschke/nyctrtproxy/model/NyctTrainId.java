package com.kurtraschke.nyctrtproxy.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NyctTrainId {
    private static final Pattern _trainIdPattern = Pattern.compile(
            "^(?<trainType>.)(?<trainRoute>[A-Z0-9]{1,2}) (?<originDepartureHours>\\d{2})(?<originDepartureMinutes>\\d{2})(?<originDeparturePlus>\\+?) ?(?<originTerminal>[A-Z0-9-]{3})/(?<destinationTerminal>[A-Z0-9-]{3})$"
    );

    private final String trainType;
    private final String trainRoute;
    private final int originDepartureTime;
    private final String origin;
    private final String destination;

    public static NyctTrainId buildFromString(String trainId) {
        Matcher m = _trainIdPattern.matcher(trainId);

        if (m.matches()) {
            String trainType = m.group("trainType");
            String trainLine = m.group("trainRoute");

            int originDepartureTime = ((Integer.parseInt(m.group("originDepartureHours")) * 60) + Integer.parseInt(m.group("originDepartureMinutes"))) * 60 + (m.group("originDeparturePlus").equals("+") ? 30 : 0);

            String origin = m.group("originTerminal");
            String destination = m.group("destinationTerminal");

            return new NyctTrainId(trainType, trainLine, originDepartureTime, origin, destination);
        } else {
            return null;
        }
    }

    private NyctTrainId(String trainType, String trainRoute, int originDepartureTime, String origin, String destination) {
        this.trainType = trainType;
        this.trainRoute = trainRoute;
        this.originDepartureTime = originDepartureTime;
        this.origin = origin;
        this.destination = destination;
    }

    public String getTrainType() {
        return trainType;
    }

    public String getTrainRoute() {
        return trainRoute;
    }

    public int getOriginDepartureTime() {
        return originDepartureTime;
    }

    public String getOrigin() {
        return origin;
    }

    public String getDestination() {
        return destination;
    }
}
