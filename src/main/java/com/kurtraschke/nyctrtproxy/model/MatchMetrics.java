/*
 * Copyright (C) 2017 Cambridge Systematics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.kurtraschke.nyctrtproxy.model;

import com.google.common.collect.Sets;

import java.util.Date;
import java.util.Set;

/**
 * Aggregate metrics per route or per feed, and handle the creation of Cloudwatch metrics objects.
 *
 * @author Simon Jacobs
 */
public class MatchMetrics {

  private int nRecordsIn = 0, nExpiredUpdates = 0;

  private int nMatchedTrips = 0, nCancelledTrips = 0, nAddedTrips = 0;
  private int nUnmatchedNoStartDate = 0, nStrictMatch = 0, nLooseMatchSameDay = 0, nLooseMatchOtherDay = 0,
    nUnmatchedNoStopMatch = 0, nLooseMatchCoercion = 0, nDuplicates = 0, nBadId = 0, nMergedTrips = 0, nMultipleMatchedTrips = 0;

  private long latency = -1;

  private Set<String> tripIds = Sets.newHashSet();

  /**
   * Add results of a match to currently aggregated metrics.
   *
   * @param result The result to add
   */
  public void add(TripMatchResult result) {
    if (result.hasResult()) {
      String tripId = result.getResult().getTrip().getId().getId();
      if (tripIds.contains(tripId)) {
        nDuplicates++;
      }
      tripIds.add(tripId);
    }
    addStatus(result.getStatus());
  }

  public void addStatus(Status status){
    switch (status) {
      case BAD_TRIP_ID:
        nAddedTrips++;
        nBadId++;
        break;
      case NO_TRIP_WITH_START_DATE:
        nAddedTrips++;
        nUnmatchedNoStartDate++;
        break;
      case NO_MATCH:
        nAddedTrips++;
        nUnmatchedNoStopMatch++;
        break;
      case STRICT_MATCH:
        nMatchedTrips++;
        nStrictMatch++;
        break;
      case LOOSE_MATCH:
        nMatchedTrips++;
        nLooseMatchSameDay++;
        break;
      case LOOSE_MATCH_ON_OTHER_SERVICE_DATE:
        nMatchedTrips++;
        nLooseMatchOtherDay++;
       break;
      case LOOSE_MATCH_COERCION:
        nMatchedTrips++;
        nLooseMatchCoercion++;
        break;
      case MERGED:
        nMatchedTrips++;
        nMergedTrips++;
        break;
      case MULTI_MATCH:
        nMultipleMatchedTrips++;
        break;
    }
  }

  /**
   * Set internal latency metric from the timestamp of a feed, relative to current time.
   *
   * @param timestamp timestamp of feed in seconds
   */
  public void reportLatency(long timestamp) {
    latency = (new Date().getTime()/1000) - timestamp;
  }

  public long getLatency() {
    return latency;
  }

  public void reportRecordsIn(int n) {
    nRecordsIn += n;
  }

  public void reportExpiredUpdates(int n) {
    nExpiredUpdates += n;
  }

  public void addCancelled() {
    nCancelledTrips++;
  }

  public int getMatchedTrips() {
    return nMatchedTrips;
  }

  public int getAddedTrips() {
    return nAddedTrips;
  }

  public int getCancelledTrips() {
    return nCancelledTrips;
  }

  public int getDuplicates() {
    return nDuplicates;
  }

  public int getMergedTrips() {
    return nMergedTrips;
  }

  public int getRecordsIn() {
    return nRecordsIn;
  }

  public int getExpiredUpdates() {
    return nExpiredUpdates;
  }

  public int getRecordsOut() {
    return nAddedTrips + nMatchedTrips + nCancelledTrips;
  }

  public int getUnmatchedNoStartDate() {
    return nUnmatchedNoStartDate;
  }

  public int getUnmatchedNoStopMatch() {
    return nUnmatchedNoStopMatch;
  }

  public int getStrictMatch() {
    return nStrictMatch;
  }

  public int getLooseMatchSameDay() {
    return nLooseMatchSameDay;
  }

  public int getLooseMatchOtherDay() {
    return nLooseMatchOtherDay;
  }

  public int getLooseMatchCoercion() {
    return nLooseMatchCoercion;
  }

  public int getBadId() {
    return nBadId;
  }

}
