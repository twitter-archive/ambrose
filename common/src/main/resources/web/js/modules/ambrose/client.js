/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/**
 * This module defines the Client class, simplifying interaction with Ambrose http server endpoints.
 */
define(['lib/jquery', 'lib/uri', './core'], function($, URI, Ambrose) {
  // Client ctor
  var Client = Ambrose.Client = function(baseUri) {
    return new Ambrose.Client.fn.init(baseUri);
  };

  /**
   * Client prototype.
   */
  Client.fn = Client.prototype = {
    /**
     * Constructs a new Client.
     *
     * @param baseUri base URI of Ambrose server to communicate with. If null, the current page's
     * url is inspected for param 'localdata'. If defined, local demo data is used. If the value of
     * 'localdata' is "small", then the small job graph and associated events data is
     * used. Otherwise the "large" demo data is used.  is used.
     */
    init: function(baseUri) {
      // default endpoint paths
      var clustersUri = 'clusters';
      var workflowsUri = 'workflows';
      var jobsUri = 'dag';
      var eventsUri = 'events';

      if (baseUri == null) {
        // look for 'localdata' param in current href
        var uri = new URI(window.location.href);
        var params = uri.search(true);
        if (params.localdata) {
          clustersUri = 'data/clusters.json';
          workflowsUri = 'data/workflows.json';
          jobsUri = 'data/jobs.json';
          eventsUri = 'data/events.json';
        }
      } else {
        // resolve relative paths given base uri
        var uri = new URI(baseUri);
        clustersUri = new URI(clustersUri).absoluteTo(uri);
        workflowsUri = new URI(workflowsUri).absoluteTo(uri);
        jobsUri = new URI(jobsUri).absoluteTo(uri);
        eventsUri = new URI(eventsUri).absoluteTo(uri);
      }

      this.clustersUri = new URI(clustersUri);
      this.workflowsUri = new URI(workflowsUri);
      this.jobsUri = new URI(jobsUri);
      this.eventsUri = new URI(eventsUri);
    },

    /**
     * Sends asynch request to server.
     */
    sendRequest: function(uri, params) {
      var self = this;
      return $.getJSON(new URI(uri).addSearch(params).unicode())
        .error(function(jqXHR, textStatus, errorThrown) {
          console.error('Request failed:', self, textStatus, errorThrown);
        })
        .success(function(data, textStatus, jqXHR) {
          console.debug('Request succeeded:', textStatus, data);
        });
    },

    /**
     * Retrieves cluster configuration from server.
     */
    getClusters: function() {
      return this.sendRequest(this.clustersUri, {});
    },

    /**
     * Submits asynchronous request for workflow summaries from server.
     *
     * @param cluster
     * @param user
     * @param status
     * @param startKey
     * @return a jQuery Promise on which success and error callbacks may be registered.
     */
    getWorkflows: function(cluster, user, status, startKey) {
      return this.sendRequest(this.workflowsUri, {
        cluster: cluster,
        user: user,
        status: status,
        startKey: startKey
      });
    },

    /**
     * Submits asynchronous request for workflow jobs from server.
     *
     * @param workflowId id of workflow for which to retrieve jobs.
     * @return a jQuery Promise on which success and error callbacks may be registered.
     */
    getJobs: function(workflowId) {
      return this.sendRequest(this.jobsUri, { workflowId: workflowId });
    },

    /**
     * Submits asynchronous request for workflow events from server.
     *
     * @param workflowId id of workflow for which to retrieve events.
     * @param lastEventId retrieve events which occurred after the event associated with this id. If
     * null, defaults to -1.
     * @return a jQuery Promise on which success and error callbacks may be registered.
     */
    getEvents: function(workflowId, lastEventId) {
      if (lastEventId == null) lastEventId = -1;
      return this.sendRequest(this.eventsUri, {
        workflowId: workflowId,
        lastEventId: lastEventId
      });
    },
  };

  // Bind prototype to ctor
  Client.fn.init.prototype = Client.fn;
  return Client;
});
