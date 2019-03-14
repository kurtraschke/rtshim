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
package com.kurtraschke.nyctrtproxy.services;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.google.inject.Inject;
import com.kurtraschke.nyctrtproxy.model.MatchMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Named;
import java.util.Date;
import java.util.Set;

/**
 * Obtain metrics per-route and per-feed, and report to Cloudwatch.
 *
 * If cloudwatch credentials are not included in the configuration, this class will simply log.
 *
 * @author Simon Jacobs
 */
public class CloudwatchProxyDataListener implements ProxyDataListener {
  private static final Logger _log = LoggerFactory.getLogger(CloudwatchProxyDataListener.class);

  @Inject(optional = true)
  @Named("cloudwatch.env")
  protected String _env;

  @Inject(optional = true)
  @Named("cloudwatch.accessKey")
  protected String _accessKey;

  @Inject(optional = true)
  @Named("cloudwatch.secretKey")
  protected String _secretKey;

  @Inject(optional = true)
  @Named("cloudwatch.region")
  protected String _region;

  protected boolean _disabled = false;

  protected boolean _verbose = false;

  protected AmazonCloudWatchAsync _client;

  protected AsyncHandler<PutMetricDataRequest, PutMetricDataResult> _handler;

  @PostConstruct
  public void init() {
    if (_secretKey == null || _accessKey == null || _env == null || _region == null) {
      _log.info("No AWS credentials supplied, disabling cloudwatch");
      _disabled = true;
      return;
    }
    BasicAWSCredentials cred = new BasicAWSCredentials(_accessKey, _secretKey);
    _client = AmazonCloudWatchAsyncClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(cred))
            .withRegion(_region)
            .build();
    _handler = new AsyncHandler<PutMetricDataRequest, PutMetricDataResult>() {
      @Override
      public void onError(Exception e) {
        _log.error("Error sending to cloudwatch: " + e);
      }

      @Override
      public void onSuccess(PutMetricDataRequest request, PutMetricDataResult putMetricDataResult) {
        // do nothing
      }
    };
  }

  @Override
  public void reportMatchesForRoute(String routeId, MatchMetrics metrics, String namespace) {
    Date timestamp = new Date();
    Dimension dim = new Dimension();
    dim.setName("route");
    dim.setValue(routeId);
    if (!processReportSubwayMatches(timestamp, dim, metrics, namespace) && !isDisabled())
      _log.info("Cloudwatch: no data reported for route={}", routeId);
    _log.info("time={}, route={}, nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}, nDuplicates={}, nMergedTrips={}", timestamp, routeId, metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips(), metrics.getDuplicates(), metrics.getMergedTrips());
  }

  @Override
  public void reportMatchesForSubwayFeed(String feedId, MatchMetrics metrics, String namespace) {
    Date timestamp = new Date();
    Dimension dim = new Dimension();
    dim.setName("feed");
    dim.setValue(feedId);
    if (!processReportSubwayMatches(timestamp, dim, metrics, namespace) && !isDisabled())
      _log.info("Cloudwatch: no data reported for feed={}", feedId);
    _log.info("time={}, feed={}, nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}, nDuplicates={}, nMergedTrips={}", timestamp, feedId, metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips(), metrics.getDuplicates(), metrics.getMergedTrips());
  }

  @Override
  public void reportMatchesForTripUpdateFeed(String feedId, MatchMetrics metrics, String namespace) {
    Date timestamp = new Date();
    Dimension dim = new Dimension();
    dim.setName("feed");
    dim.setValue(feedId);
    if (!processReportTripUpdateMatches(timestamp, dim, metrics, namespace) && !isDisabled())
      _log.info("Cloudwatch: no data reported for feed={}", feedId);
    _log.info("time={}, feed={}, nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}", timestamp, feedId, metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips());
  }

  @Override
  public void reportMatchesTotal(MatchMetrics metrics, String namespace) {
    Date timestamp = new Date();
    if (!processReportSubwayMatches(timestamp, null, metrics, namespace) && !isDisabled())
      _log.info("Cloudwatch: no data reported for total metrics.");
    _log.info("time={} total: nMatchedTrips={}, nAddedTrips={}, nCancelledTrips={}, nDuplicates={}, nMergedTrips={}", timestamp, metrics.getMatchedTrips(), metrics.getAddedTrips(), metrics.getCancelledTrips(), metrics.getDuplicates(), metrics.getMergedTrips());
  }

  private boolean processReportTripUpdateMatches(Date timestamp, Dimension dim, MatchMetrics metrics, String namespace) {
    if (isDisabled())
      return false;

    Set<MetricDatum> data = metrics.getMinimalReportedMetrics(dim, timestamp);
    if (data.isEmpty())
      return false;

    publishMetric(namespace, data);

    return true;
  }

  private boolean processReportSubwayMatches(Date timestamp, Dimension dim, MatchMetrics metrics, String namespace) {
    if (isDisabled())
      return false;

    Set<MetricDatum> data = metrics.getReportedMetrics(_verbose, dim, timestamp);
    if (data.isEmpty())
      return false;

    publishMetric(namespace, data);

    return true;
  }

  @Override
  public void publishMetric(String namespace, Set<MetricDatum> data) {
    PutMetricDataRequest request = new PutMetricDataRequest();
    request.setMetricData(data);
    if(namespace != null) {
      request.setNamespace(namespace + ":" + _env);
    }
    _client.putMetricDataAsync(request, _handler);
  }

  public void setEnv(String env) {
    _env = env;
  }

  public void setAccessKey(String accessKey) {
    _accessKey = accessKey;
  }

  public void setSecretKey(String secretKey) {
    _secretKey = secretKey;
  }

  public void setRegion(String region) {
    _region = region;
  }

  public void setVerbose(boolean verbose) {
    _verbose = verbose;
  }

  public boolean isDisabled() {
    return _disabled;
  }
}
