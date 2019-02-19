package com.kurtraschke.nyctrtproxy.services;

import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtimeOneBusAway;
import com.kurtraschke.nyctrtproxy.model.DirectionEntry;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.services.GtfsDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kurtraschke.nyctrtproxy.util.CsvReaderUtil.readCsv;

public class DirectionsService {

    private static final Logger _log = LoggerFactory.getLogger(GtfsRelationalDaoProvider.class);

    private GtfsDataService _gtfs;

    @Inject
    @Named("directions.csvPath")
    private String _directionsCsv;

    @Inject
    public void setGtfsDataService(GtfsDataService gtfs) {
        _gtfs = gtfs;
    }

    private Map<String, DirectionEntry> dirByStation = new HashMap<>();

    public void init() {
        List<DirectionEntry> stationDirections = readCsv(DirectionEntry.class, _directionsCsv);

        for (DirectionEntry dir : stationDirections) {
            if (dirByStation.get(dir.getGtfsStopId()) != null) {
                _log.error("Duplicate station: {}", dir.getGtfsStopId());
            }
            dirByStation.put(dir.getGtfsStopId(), dir);
        }
    }

    public void fillStopHeadSigns(List<GtfsRealtime.TripUpdate.StopTimeUpdate.Builder> stopTimeUpdates){

        for (GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stu : stopTimeUpdates) {

            String stopId = stu.getStopId();
            String stationId = getStationForStopId(stopId);
            DirectionEntry dir = dirByStation.get(stationId);

            if (dir == null) {
                _log.debug("Missing station ID = {}", stationId);
                continue;
            }

            String direction = stopId.substring(stopId.length() - 1);
            String headsign = null;

            if ("N".equals(direction)) {
                headsign = dir.getRailroadNorthDescriptor();
            } else if ("S".equals(direction)) {
                headsign = dir.getRailroadSouthDescriptor();
            }
            if (headsign != null && !headsign.equals("n/a")) {
                GtfsRealtimeOneBusAway.OneBusAwayStopTimeUpdate stopTimeUpdate = GtfsRealtimeOneBusAway.OneBusAwayStopTimeUpdate
                        .newBuilder().setStopHeadsign(headsign).build();
                stu.setExtension(GtfsRealtimeOneBusAway.obaStopTimeUpdate, stopTimeUpdate);
            }
        }
    }

    private String getStationForStopId(String stopId){
        for(Agency agency : _gtfs.getAllAgencies()){
            AgencyAndId stopAid = new AgencyAndId(agency.getId(), stopId);
            Stop stop = _gtfs.getStopForId(stopAid);
            if(stop != null){
                return stop.getParentStation();
            }
        }
        return null;
    }

    public void setDirectionCsv(String directionCsv) {
        _directionsCsv = directionCsv;
    }

    public Map<String, DirectionEntry> getDirections(){
        return dirByStation;
    }

    public DirectionEntry getDirection(String stopId){
        return dirByStation.get(stopId);
    }
}
