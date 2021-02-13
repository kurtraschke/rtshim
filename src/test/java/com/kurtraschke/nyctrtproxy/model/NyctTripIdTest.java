package com.kurtraschke.nyctrtproxy.model;

import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

//Test all of the trip ID formats currently in use on the network; also exercise Flushing direction inference
public class NyctTripIdTest {

    private static TripDescriptor td(String tripId) {
        return td(tripId, "");
    }

    private static TripDescriptor td(String tripId, String trainId) {
        return TripDescriptor
                .newBuilder()
                .setTripId(tripId)
                .setExtension(GtfsRealtimeNYCT.nyctTripDescriptor, GtfsRealtimeNYCT.NyctTripDescriptor.newBuilder().setTrainId(trainId).build())
                .build();
    }

    @Test
    public void buildFromTripDescriptorADivision() {
        NyctTripId tripId = NyctTripId.buildFromTripDescriptor(td("118600_1..S03R"));

        assertEquals(118600, tripId.getOriginDepartureTime());
        assertEquals("1..S", tripId.getPathId());
        assertEquals("03R", tripId.getNetworkId());
        assertEquals("1", tripId.getRouteId());
        assertEquals("S", tripId.getDirection());
    }

    @Test
    public void buildFromTripDescriptorBDivision() {
        NyctTripId tripId = NyctTripId.buildFromTripDescriptor(td("060800_C..S"));

        assertEquals(60800, tripId.getOriginDepartureTime());
        assertEquals("C..S", tripId.getPathId());
        assertEquals("C", tripId.getRouteId());
        assertEquals("S", tripId.getDirection());
    }

    @Test
    public void buildFromTripDescriptorCanarsie() {
        NyctTripId tripId = NyctTripId.buildFromTripDescriptor(td("044850_L..N"));

        assertEquals(44850, tripId.getOriginDepartureTime());
        assertEquals("L..N", tripId.getPathId());
        assertEquals("L", tripId.getRouteId());
        assertEquals("N", tripId.getDirection());
    }

    @Test
    public void buildFromTripDescriptorFlushing() {
        NyctTripId tripId = NyctTripId.buildFromTripDescriptor(td("117900_7..MAIN ST34", "07 1939 MST/34H"));

        assertEquals(117900, tripId.getOriginDepartureTime());
        assertEquals("7..", tripId.getPathId());
        assertEquals("MAIN ST34", tripId.getNetworkId());
        assertEquals("7", tripId.getRouteId());
        assertEquals("S", tripId.getDirection());
    }

    @Test
    public void buildFromTripDescriptorFlushingExpress() {
        NyctTripId tripId = NyctTripId.buildFromTripDescriptor(td("116750_7X..34ST-11M", "07 1927+ 34H/MST"));

        assertEquals(116750, tripId.getOriginDepartureTime());
        assertEquals("7X.", tripId.getPathId());
        assertEquals("34ST-11M", tripId.getNetworkId());
        assertEquals("7X", tripId.getRouteId());
        assertEquals("N", tripId.getDirection());
    }


    @Test
    public void inferFlushingTripDirection() {
        assertEquals("S", NyctTripId.inferFlushingDirection("07 0808+ MST/34H"));
        assertEquals("N", NyctTripId.inferFlushingDirection("07 1131 34H/MST"));
        assertEquals("N", NyctTripId.inferFlushingDirection("07 1709 34H/WPT"));
    }

}
