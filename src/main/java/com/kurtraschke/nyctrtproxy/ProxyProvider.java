/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kurtraschke.nyctrtproxy;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import com.kurtraschke.nyctrtproxy.services.ProxyDataListener;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeFullUpdate;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeGuiceBindingTypes.TripUpdates;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeSink;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ExtensionRegistry;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor.ScheduleRelationship;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtimeNYCT;
import com.kurtraschke.nyctrtproxy.model.ActivatedTrip;
import com.kurtraschke.nyctrtproxy.model.NyctTripId;
import com.kurtraschke.nyctrtproxy.services.TripActivator;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Named;

/**
 *
 * @author kurt
 */
public class ProxyProvider {

  private static final org.slf4j.Logger _log = LoggerFactory.getLogger(ProxyProvider.class);

  private static final ExtensionRegistry _extensionRegistry;

  private TripActivator _tripActivator;

  private GtfsRealtimeSink _tripUpdatesSink;

  private String _key;

  private HttpClientConnectionManager _connectionManager;

  private CloseableHttpClient _httpClient;

  private ScheduledExecutorService _scheduledExecutorService;

  private ScheduledFuture _updater;

  private ProxyDataListener _listener;

  private static final Map<Integer, Set<String>> routeBlacklistByFeed = ImmutableMap.of(1, ImmutableSet.of("D", "N", "Q"));

  private static final Set<String> routesUsingAlternateIdFormat = ImmutableSet.of("SI", "L", "N", "Q", "R", "W", "B", "D");

  private static final Set<String> routesNeedingFixup = ImmutableSet.of("SI", "N", "Q", "R", "W");

  private static final Map<Integer, Map<String, String>> realtimeToStaticRouteMapByFeed = ImmutableMap.of(1, ImmutableMap.of("S", "GS"));

  static {
    _extensionRegistry = ExtensionRegistry.newInstance();
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctFeedHeader);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctTripDescriptor);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctStopTimeUpdate);
  }

  @Inject
  public void setTripActivator(TripActivator tripActivator) {
    _tripActivator = tripActivator;
  }

  @Inject
  public void setTripUpdatesSink(@TripUpdates GtfsRealtimeSink tripUpdatesSink) {
    _tripUpdatesSink = tripUpdatesSink;
  }

  @Inject
  public void setHttpClientConnectionManager(HttpClientConnectionManager connectionManager) {
    _connectionManager = connectionManager;
  }

  @Inject
  public void setScheduledExecutorService(ScheduledExecutorService service) {
    _scheduledExecutorService = service;
  }

  @Inject
  public void setKey(@Named("NYCT.key") String key) {
    _key = key;
  }

  @Inject(optional = true)
  public void setListener(ProxyDataListener listener) {
    _listener = listener;
  }

  @PostConstruct
  public void start() {
    _httpClient = HttpClientBuilder.create().setConnectionManager(_connectionManager).build();
    _updater = _scheduledExecutorService.scheduleWithFixedDelay(this::update, 0, 60, TimeUnit.SECONDS);

  }

  @PreDestroy
  public void stop() {
    _updater.cancel(false);
    _scheduledExecutorService.shutdown();
    _connectionManager.shutdown();
  }

  public void update() {
    _log.info("doing update");

    GtfsRealtimeFullUpdate grfu = new GtfsRealtimeFullUpdate();

    List<TripUpdate> tripUpdates = Lists.newArrayList();

    for (int feedId : Arrays.asList(1, 2, 11, 16, 21)) { // 1, 2, 11, 16, 21
      URI feedUrl;

      try {
        URIBuilder ub = new URIBuilder("http://datamine.mta.info/mta_esi.php");

        ub.addParameter("key", _key);
        ub.addParameter("feed_id", Integer.toString(feedId));

        feedUrl = ub.build();
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }

      HttpGet get = new HttpGet(feedUrl);

      try {
        CloseableHttpResponse response = _httpClient.execute(get);
        try (InputStream streamContent = response.getEntity().getContent()) {
          tripUpdates.addAll(processFeed(feedId, FeedMessage.parseFrom(streamContent, _extensionRegistry)));
        }
      } catch (Exception ex) {
        _log.warn("Exception while fetching source feed " + feedUrl, ex);
      }
    }

    for (TripUpdate tu : tripUpdates) {
      FeedEntity.Builder feb = FeedEntity.newBuilder();
      feb.setTripUpdate(tu);
      feb.setId(tu.getTrip().getTripId());
      grfu.addEntity(feb.build());
    }

    _tripUpdatesSink.handleFullUpdate(grfu);
  }

  private static NyctTripId parseTripId(String routeId, String tripId, int feedId) {
    if (!routesUsingAlternateIdFormat.contains(routeId)) {
      return NyctTripId.buildFromString(tripId);
    } else {
      return NyctTripId.buildFromAlternateString(tripId);
    }
  }

  public List<TripUpdate> processFeed(Integer feedId, FeedMessage fm) {
    final Map<String, String> realtimeToStaticRouteMap = realtimeToStaticRouteMapByFeed
            .getOrDefault(feedId, Collections.emptyMap());

    final Map<String, List<GtfsRealtime.TripUpdate>> tripUpdatesByRoute = fm
            .getEntityList()
            .stream()
            .filter(GtfsRealtime.FeedEntity::hasTripUpdate)
            .map(GtfsRealtime.FeedEntity::getTripUpdate)
            .collect(Collectors.groupingBy(tu -> {
              String routeId = tu.getTrip().getRouteId();
              return realtimeToStaticRouteMap
                      .getOrDefault(routeId, routeId);
            }));

    List<TripUpdate> ret = Lists.newArrayList();

    int nMatchedFeed = 0, nAddedFeed = 0, nCancelledFeed = 0;

    for (GtfsRealtimeNYCT.TripReplacementPeriod trp : fm.getHeader()
            .getExtension(GtfsRealtimeNYCT.nyctFeedHeader)
            .getTripReplacementPeriodList()) {
      if (routeBlacklistByFeed.getOrDefault(feedId, Collections.emptySet()).contains(trp.getRouteId()))
        continue;
      GtfsRealtime.TimeRange range = trp.getReplacementPeriod();

      Date start = range.hasStart() ? new Date(range.getStart() * 1000) : new Date();
      Date end = range.hasEnd() ? new Date(range.getEnd() * 1000) : new Date();

      Set<String> routeIds = Arrays.stream(trp.getRouteId().split(", ?"))
              .map(routeId -> {
                return realtimeToStaticRouteMap
                        .getOrDefault(routeId, routeId);
              }).collect(Collectors.toSet());

      Multimap<String, ActivatedTrip> staticTripsForRoute = ArrayListMultimap.create();
      for (ActivatedTrip trip : _tripActivator.getTripsForRangeAndRoutes(start, end, routeIds).collect(Collectors.toList())) {
        staticTripsForRoute.put(trip.getTrip().getRoute().getId().getId(), trip);
      }

      Set<String> matchedTripIds = new HashSet<>();

      for (String routeId : routeIds) {
        Collection<ActivatedTrip> staticTrips = staticTripsForRoute.get(routeId);

        int nTripUpdatesFromStatic = 0, nTripUpdatesAdded = 0, nTripUpdatesCancelled = 0;

        List<TripUpdate> tripUpdates = tripUpdatesByRoute.get(routeId);
        for (TripUpdate tu : tripUpdates) {
          TripUpdate.Builder tub = TripUpdate.newBuilder(tu);
          TripDescriptor.Builder tb = tub.getTripBuilder();

          tb.setRouteId(realtimeToStaticRouteMap
                  .getOrDefault(tb.getRouteId(), tb.getRouteId()));

          Optional<ActivatedTrip> matchedStaticTrip;

          NyctTripId rtid = parseTripId(tb.getRouteId(), tb.getTripId(), feedId);

          if (routesNeedingFixup.contains(tb.getRouteId())) {
            tb.setStartDate(tb.getStartDate().substring(0, 10).replace("-", ""));

            tub.getStopTimeUpdateBuilderList().forEach(stub -> {
              stub.setStopId(stub.getStopId() + rtid.getDirection());
            });

            tb.setTripId(rtid.toString());
          }

          Stream<ActivatedTrip> candidateTrips = staticTrips
                  .stream()
                  .filter(at -> at.getServiceDate().getAsString().equals(tb.getStartDate()));

          if (routesUsingAlternateIdFormat.contains(tb.getRouteId())) {
            matchedStaticTrip = candidateTrips
                    .filter(at -> {
                      NyctTripId stid = at.getParsedTripId();

                      return stid.getOriginDepartureTime() == rtid.getOriginDepartureTime()
                              && stid.getDirection().equals(rtid.getDirection());
                    })
                    .findFirst();
          } else {
            matchedStaticTrip = candidateTrips
                    .filter(at -> {
                      NyctTripId stid = at.getParsedTripId();

                      return stid.getOriginDepartureTime() == rtid.getOriginDepartureTime()
                              && stid.getPathId().equals(rtid.getPathId());
                    })
                    .findFirst();
          }

          if (matchedStaticTrip.isPresent()) {
            String staticTripId = matchedStaticTrip.get().getTrip().getId().getId();
            matchedTripIds.add(staticTripId);
            tb.setTripId(staticTripId);
            nTripUpdatesFromStatic++;
          } else {
            tb.setScheduleRelationship(ScheduleRelationship.ADDED);
            nTripUpdatesAdded++;
          }
          ret.add(tub.build());
        }

        for (ActivatedTrip at : staticTrips) {
          if (!matchedTripIds.contains(at.getTrip().getId().getId())) {
            TripUpdate.Builder tub = TripUpdate.newBuilder();
            TripDescriptor.Builder tdb = tub.getTripBuilder();
            tdb.setTripId(at.getTrip().getId().getId());
            tdb.setRouteId(at.getTrip().getRoute().getId().getId());
            tdb.setStartDate(at.getServiceDate().getAsString());
            tdb.setScheduleRelationship(ScheduleRelationship.CANCELED);
            ret.add(tub.build());
            nTripUpdatesCancelled++;
          }
        }

        if (_listener != null)
          _listener.reportMatchesForRoute(routeId, nTripUpdatesFromStatic, nTripUpdatesAdded, nTripUpdatesCancelled);
        nMatchedFeed += nTripUpdatesFromStatic;
        nAddedFeed += nTripUpdatesAdded;
        nCancelledFeed += nTripUpdatesCancelled;
      }
    }

    if (_listener != null)
      _listener.reportMatchesForFeed(feedId.toString(), nMatchedFeed, nAddedFeed, nCancelledFeed);
    return ret;
  }
}
