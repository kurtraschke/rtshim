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

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.kurtraschke.nyctrtproxy.model.MatchMetrics;

import java.util.Set;

/**
 * Listener for the results of TripUpdateProcessor.
 *
 * @author Simon Jacobs
 */
public interface ProxyDataListener {
  void reportMatchesForRoute(String routeId, MatchMetrics metrics, String namespace);
  void reportMatchesForSubwayFeed(String feedId, MatchMetrics metrics, String namespace);
  void reportMatchesForTripUpdateFeed(String feedId, MatchMetrics metrics, String namespace);
  void reportMatchesTotal(MatchMetrics metrics, String namespace);
  void publishMetric(String namespace, Set<MetricDatum> data);
}
