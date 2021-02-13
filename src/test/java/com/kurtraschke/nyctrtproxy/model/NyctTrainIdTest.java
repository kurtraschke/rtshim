package com.kurtraschke.nyctrtproxy.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class NyctTrainIdTest {

    @Test
    public void buildFromStringADivision() {
        NyctTrainId trainId = NyctTrainId.buildFromString("/6 2259+ BBR/PEL");

        assertNotNull(trainId);
        assertEquals("/", trainId.getTrainType());
        assertEquals("6", trainId.getTrainRoute());
        assertEquals(82770, trainId.getOriginDepartureTime());
        assertEquals("BBR", trainId.getOrigin());
        assertEquals("PEL", trainId.getDestination());
    }

    @Test
    public void buildFromStringBDivision() {
        NyctTrainId trainId = NyctTrainId.buildFromString("1FS 2309 PPK/P-A");

        assertNotNull(trainId);
        assertEquals("1", trainId.getTrainType());
        assertEquals("FS", trainId.getTrainRoute());
        assertEquals(83340, trainId.getOriginDepartureTime());
        assertEquals("PPK", trainId.getOrigin());
        assertEquals("P-A", trainId.getDestination());
    }


    //Note no space between the plus and the origin stop; this is (so far) the only exception to the standard format.
    @Test
    public void buildFromStringCanarsie() {
        NyctTrainId trainId = NyctTrainId.buildFromString("0L 2300+8AV/MYR");

        assertNotNull(trainId);
        assertEquals("0", trainId.getTrainType());
        assertEquals("L", trainId.getTrainRoute());
        assertEquals(82830, trainId.getOriginDepartureTime());
        assertEquals("8AV", trainId.getOrigin());
        assertEquals("MYR", trainId.getDestination());
    }

    @Test
    public void buildFromStringFlushing() {
        NyctTrainId trainId = NyctTrainId.buildFromString("07 0808+ MST/34H");

        assertNotNull(trainId);
        assertEquals("0", trainId.getTrainType());
        assertEquals("7", trainId.getTrainRoute());
        assertEquals(29310, trainId.getOriginDepartureTime());
        assertEquals("MST", trainId.getOrigin());
        assertEquals("34H", trainId.getDestination());
    }
}
