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

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
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

  /**
   * Return a set of Cloudwatch metric data based on currently aggregated data.
   *
   * @param dim Dimension for the returned metrics (likely route or feed)
   * @param timestamp Timestamp to use for returned metrics
   * @return Set of Cloudwatch metrics
   */
  public Set<MetricDatum> getReportedMetrics(boolean verbose, Dimension dim, Date timestamp) {

    Set<MetricDatum> data = Sets.newHashSet();
    addLatencyMetrics(data, dim, timestamp);

    if (nMatchedTrips + nAddedTrips > 0) {
      data.addAll(verbose ? getMatchMetricsVerbose(dim, timestamp) : getMatchMetricsNonVerbose(dim, timestamp));
    }

    return data;
  }

  public Set<MetricDatum> getMinimalReportedMetrics(Dimension dim, Date timestamp) {

    Set<MetricDatum> data = Sets.newHashSet();
    addLatencyMetrics(data, dim, timestamp);

    data.addAll(getMatchMetricsMinimal(dim, timestamp));

    return data;
  }

  private void addLatencyMetrics(Set<MetricDatum> data, Dimension dim, Date timestamp){
    if (latency >= 0) {
      MetricDatum dLatency = new MetricDatum().withMetricName("Latency")
              .withTimestamp(timestamp)
              .withValue((double) latency)
              .withUnit(StandardUnit.Seconds);
      if (dim != null) {
        dLatency = dLatency.withDimensions(dim);
      }
      data.add(dLatency);
    }
  }

  private Set<MetricDatum> getMatchMetricsMinimal(Dimension dim, Date timestamp){
    MetricDatum dRecordsIn = metricCount(timestamp, "RecordsIn", nRecordsIn, dim);
    MetricDatum dAdded = metricCount(timestamp, "AddedTrips", nAddedTrips, dim);
    MetricDatum dMatched = metricCount(timestamp, "MatchedTrips", nMatchedTrips, dim);
    MetricDatum dRecordsOut = metricCount(timestamp, "RecordsOut", nMatchedTrips, dim);
    return Sets.newHashSet(dRecordsIn, dMatched, dAdded, dRecordsOut);
  }

  private Set<MetricDatum> getMatchMetricsNonVerbose(Dimension dim, Date timestamp) {
    MetricDatum dRecordsIn = metricCount(timestamp, "RecordsIn", nRecordsIn, dim);
    MetricDatum dExpiredUpdates = metricCount(timestamp, "ExpiredUpdates", nExpiredUpdates, dim);
    MetricDatum dMatched = metricCount(timestamp, "MatchedTrips", nMatchedTrips, dim);
    MetricDatum dAdded = metricCount(timestamp, "AddedTrips", nAddedTrips, dim);
    MetricDatum dCancelled = metricCount(timestamp, "CancelledTrips", nCancelledTrips, dim);
    MetricDatum dMerged = metricCount(timestamp, "MergedTrips", nMergedTrips, dim);
    MetricDatum dRecordsOut = metricCount(timestamp, "RecordsOut", nAddedTrips + nMatchedTrips + nCancelledTrips, dim);
    return Sets.newHashSet(dRecordsIn, dExpiredUpdates, dMatched, dAdded, dCancelled, dMerged, dRecordsOut);
  }

  private Set<MetricDatum> getMatchMetricsVerbose(Dimension dim, Date timestamp) {
    double nRt = nMatchedTrips + nAddedTrips;
    double nMatchedRtPct = ((double) nMatchedTrips) / nRt;

    double nUnmatchedWithoutStartDatePct = ((double) nUnmatchedNoStartDate) / nRt;
    double nUnmatchedNoStopMatchPct = ((double) nUnmatchedNoStopMatch) / nRt;
    double nStrictMatchPct = ((double) nStrictMatch) / nRt;
    double nLooseMatchSameDayPct = ((double) nLooseMatchSameDay) / nRt;
    double nLooseMatchOtherDayPct = ((double) nLooseMatchOtherDay) / nRt;
    double nLooseMatchCoercionPct = ((double) nLooseMatchCoercion) / nRt;
    double nMergedPct = ((double) nMergedTrips) / nRt;

    MetricDatum dMatched = metricCount(timestamp, "MatchedTrips", nMatchedTrips, dim);
    MetricDatum dAdded = metricCount(timestamp, "AddedTrips", nAddedTrips, dim);
    MetricDatum dCancelled = metricCount(timestamp, "CancelledTrips", nCancelledTrips, dim);
    MetricDatum dDuplicateTrips = metricCount(timestamp, "DuplicateTripMatches", nDuplicates, dim);
    MetricDatum dBadId = metricCount(timestamp, "UnmatchedBadId", nBadId, dim);
    MetricDatum dMerged = metricCount(timestamp, "MergedTrips", nMergedTrips, dim);

    MetricDatum dMatchedRtPct = metricPct(timestamp, "MatchedRtTripsPct", nMatchedRtPct, dim);
    MetricDatum dUnmatchedWithoutStartDatePct = metricPct(timestamp, "UnmatchedWithoutStartDatePct", nUnmatchedWithoutStartDatePct, dim);
    MetricDatum dUnmatchedNoStopMatchPct = metricPct(timestamp, "UnmatchedNoStopMatchPct", nUnmatchedNoStopMatchPct, dim);
    MetricDatum dStrictMatchPct = metricPct(timestamp, "StrictMatchPct", nStrictMatchPct, dim);
    MetricDatum dLooseMatchSameDayPct = metricPct(timestamp, "LooseMatchSameDayPct", nLooseMatchSameDayPct, dim);
    MetricDatum dLooseMatchOtherDayPct = metricPct(timestamp, "LooseMatchOtherDayPct", nLooseMatchOtherDayPct, dim);
    MetricDatum dLooseMatchCoercionPct = metricPct(timestamp, "LooseMatchCoercionPct", nLooseMatchCoercionPct, dim);
    MetricDatum dMergedPct = metricPct(timestamp, "MergedTripsPct", nMergedPct, dim);

    return Sets.newHashSet(dMatched, dAdded, dCancelled, dMatchedRtPct, dUnmatchedWithoutStartDatePct,
            dStrictMatchPct, dLooseMatchSameDayPct, dLooseMatchOtherDayPct, dUnmatchedNoStopMatchPct,
            dLooseMatchCoercionPct, dDuplicateTrips, dBadId, dMerged, dMergedPct);
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

  private static MetricDatum metricCount(Date timestamp, String name, int value, Dimension dim) {
    MetricDatum d = new MetricDatum().withMetricName(name)
            .withTimestamp(timestamp)
            .withValue((double) value)
            .withUnit(StandardUnit.Count);
    if (dim != null)
      d.withDimensions(dim);
    return d;
  }

  private static MetricDatum metricPct(Date timestamp, String name, double value, Dimension dim) {
    MetricDatum d = new MetricDatum().withMetricName(name)
            .withTimestamp(timestamp)
            .withValue(value * 100.0)
            .withUnit(StandardUnit.Percent);
    if (dim != null)
      d.withDimensions(dim);
    return d;
  }
}
