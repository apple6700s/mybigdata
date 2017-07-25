/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastory.commons3.es.bulk_writer.transport.support;

import com.datastory.commons3.es.bulk_writer.action.PreferredNodeActionRequest;
import com.datastory.commons3.es.bulk_writer.transport.DsTransportClientNodesService;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.*;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 *
 */
public class DsTransportProxyClient {

    private final DsTransportClientNodesService nodesService;
    private final ImmutableMap<Action, TransportActionNodeProxy> proxies;

    @Inject
    public DsTransportProxyClient(Settings settings, TransportService transportService, DsTransportClientNodesService nodesService, Map<String, GenericAction> actions) {
        this.nodesService = nodesService;
        MapBuilder<Action, TransportActionNodeProxy> actionsBuilder = new MapBuilder<>();
        for (GenericAction action : actions.values()) {
            if (action instanceof Action) {
                actionsBuilder.put((Action) action, new TransportActionNodeProxy(settings, action, transportService));
            }
        }
        this.proxies = actionsBuilder.immutableMap();
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(
            final Action<Request, Response, RequestBuilder> action,
            final Request request,
            ActionListener<Response> listener) {

        final TransportActionNodeProxy<Request, Response> proxy = proxies.get(action);
        DiscoveryNode preferredNode = null;
        if (request instanceof PreferredNodeActionRequest) {
            preferredNode = ((PreferredNodeActionRequest) request).getPreferredNode();
        }

        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<Response>() {
            @Override
            public void doWithNode(DiscoveryNode node, ActionListener<Response> listener) {
                proxy.execute(node, request, listener);
            }
        }, listener, preferredNode);
    }
}
