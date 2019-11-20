/*
 * Copyright (C) 2015 Kurt Raschke <kurt@kurtraschke.com>
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
package com.kurtraschke.nyctrtproxy;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.kurtraschke.nyctrtproxy.model.MatchMetrics;
import com.kurtraschke.nyctrtproxy.services.ProxyDataListener;
import com.kurtraschke.nyctrtproxy.services.TripUpdateProcessor;
import org.onebusaway.cloud.api.ExternalServices;
import org.onebusaway.cloud.api.ExternalServicesBridgeFactory;
import org.onebusaway.cloud.api.InputStreamConsumer;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeFullUpdate;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeGuiceBindingTypes.TripUpdates;
import org.onebusaway.gtfs_realtime.exporter.GtfsRealtimeSink;

import com.google.protobuf.ExtensionRegistry;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtimeNYCT;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Named;

/**
 *
 * @author kurt
 */
public class ProxyProvider {

  private static final org.slf4j.Logger _log = LoggerFactory.getLogger(ProxyProvider.class);

  private static final String BASE_URL = "http://datamine.mta.info/mta_esi.php";

  private static final ExtensionRegistry _extensionRegistry;

  private GtfsRealtimeSink _tripUpdatesSink;

  private String _key;

  private String _url = BASE_URL;

  private HttpClientConnectionManager _connectionManager;

  private CloseableHttpClient _httpClient;

  private ScheduledExecutorService _scheduledExecutorService;

  private ScheduledFuture _updater;

  private TripUpdateProcessor _processor;

  private ProxyDataListener _listener;

  private int _nTries = 5;

  private int _refreshRate = 60;

  private int _retryDelay = 5;

  public void setBaseUrl(String url) {
    _url = url;
  }

  private Map<String, String> _overrideFeedToUrlMap = new HashMap<>();
  public java.util.Map<String, String> getOverrideFeedToUrlMap() {
    return _overrideFeedToUrlMap;
  }
  public void setOverrideFeedToUrl(Map<String, String> kv) {
    for (String key: kv.keySet()) {
      _overrideFeedToUrlMap.put(key, kv.get(key));
    }
  }
  private ExternalServices _es;
  private ExternalServices getExternalServices() {
    if (_es == null) {
      _es = new ExternalServicesBridgeFactory().getExternalServices();
    }
    return _es;
  }

  private Map<String, String> _feedToProfileMap = new HashMap<>();
  public Map<String, String> getFeedToProfileMap() {
    return _feedToProfileMap;
  }
  public void setFeedToProfile(Map<String, String> kv) {
    for (String key: kv.keySet()) {
      _feedToProfileMap.put(key, kv.get(key));
    }
  }

  private Map<String, String> _feedToRegionMap = new HashMap<>();
  public Map<String, String> getFeedToRegionMap() {
    return _feedToRegionMap;
  }
  public void setFeedToRegion(Map<String, String> kv) {
    for (String key: kv.keySet()) {
      _feedToRegionMap.put(key, kv.get(key));
    }
  }

  private List<Integer> _feedIds = Arrays.asList(1, 2, 11, 16, 21);

  static {
    _extensionRegistry = ExtensionRegistry.newInstance();
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctFeedHeader);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctTripDescriptor);
    _extensionRegistry.add(GtfsRealtimeNYCT.nyctStopTimeUpdate);
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
  public void setNTries(@Named("NYCT.nTries") int nTries) {
    _nTries = nTries;
  }

  @Inject(optional = true)
  public void setFeedIds(@Named("NYCT.feedIds") String json) {
    Type type = new TypeToken<List<Integer>>(){}.getType();
    _feedIds = new Gson().fromJson(json, type);
  }

  @Inject(optional = true)
  public void setRefreshRate(@Named("NYCT.refreshRate") int refreshRate) {
    _refreshRate = refreshRate;
  }

  @Inject(optional = true)
  public void setRetryDelay(@Named("NYCT.retryDelay") int retryDelay) {
    _retryDelay = retryDelay;
  }

  @Inject
  public void setTripUpdateProcessor(TripUpdateProcessor processor) {
    _processor = processor;
  }

  @Inject(optional = true)
  public void setListener(ProxyDataListener listener) {
    _listener = listener;
  }

  @PostConstruct
  public void start() {
    _httpClient = HttpClientBuilder.create().setConnectionManager(_connectionManager).build();
    if (_scheduledExecutorService != null)
      _updater = _scheduledExecutorService.scheduleWithFixedDelay(this::update, 0, _refreshRate, TimeUnit.SECONDS);
  }

  @PreDestroy
  public void stop() {
    if (_scheduledExecutorService != null) {
      _updater.cancel(false);
      _scheduledExecutorService.shutdown();
    }
    _connectionManager.shutdown();
  }

  public void update() {
    _log.info("doing update");

    GtfsRealtimeFullUpdate grfu = new GtfsRealtimeFullUpdate();

    List<TripUpdate> tripUpdates = Lists.newArrayList();

    MatchMetrics totalMetrics = new MatchMetrics();

    // For each feed ID, read in GTFS-RT, process trip updates, push to output.
    for (int feedId : _feedIds) {
      URI feedUrl;

      try {
        feedUrl = getFeed(String.valueOf(feedId));
        _log.info("using url |" + feedUrl + "| for feedId=" + feedId);

      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }

      FeedMessage message = null;
      for (int tries = 0; tries < _nTries; tries++) {
        try {

          try (InputStream streamContent = getStream(feedUrl, String.valueOf(feedId))) {
           message = FeedMessage.parseFrom(streamContent, _extensionRegistry);
           if (!message.getEntityList().isEmpty())
            break;
           Thread.sleep(_retryDelay * 1000);
          }
        } catch (Exception e) {
          _log.error("Error parsing protocol buffer for feed={}. try={}, retry={}. Error={}",
                  feedId, tries, tries < _nTries, e.getMessage());
        }
      }

      if (message != null) {
        try {
          tripUpdates.addAll(_processor.processFeed(feedId, message, totalMetrics));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    for (TripUpdate tu : tripUpdates) {
      FeedEntity.Builder feb = FeedEntity.newBuilder();
      feb.setTripUpdate(tu);
      feb.setId(tu.getTrip().getTripId());
      grfu.addEntity(feb.build());
    }

    _log.info("writing {} total trip updates", tripUpdates.size());

    _tripUpdatesSink.handleFullUpdate(grfu);

    if (_listener != null)
      _listener.reportMatchesTotal(totalMetrics, _processor.getCloudwatchNamespace());
  }
  private InputStream getStream(URI feedUrl, String feedId) throws IOException {
    if (feedUrl.getScheme().toLowerCase().startsWith("http")) {
      HttpGet get = new HttpGet(feedUrl);
      CloseableHttpResponse response = _httpClient.execute(get);
      InputStream streamContent = response.getEntity().getContent();
      return streamContent;
    }
    if (feedUrl.getScheme().toLowerCase().startsWith("s3")) {
      ExternalServices es = getExternalServices();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      es.getFileAsStream(feedUrl.toString(), new InputStreamConsumer() {
        @Override
        public void accept(InputStream inputStream) throws IOException {
          byte[] buffer = new byte[1024];
          int len;
          while ((len = inputStream.read(buffer)) > -1 ) {
            baos.write(buffer, 0, len);
          }
          baos.flush();
        }
      }, _feedToProfileMap.get(feedId),
        _feedToRegionMap.get(feedId));
      return new ByteArrayInputStream(baos.toByteArray());
    }
    throw new IllegalArgumentException("unexpected scheme " + feedUrl.getScheme() + " for url " + feedUrl);

  }

  private URI getFeed(String feedId) throws URISyntaxException {
    String url = _overrideFeedToUrlMap.get(feedId);
    if (url == null) {
      URIBuilder ub = new URIBuilder(_url);
      // use api key as param
      ub.addParameter("key", _key);
      ub.addParameter("feed_id", feedId);
      return ub.build();
    }
    return new URIBuilder(url).build();
  }

}
