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
define(['jquery', 'uri', './core'], function($, URI, Ambrose) {
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
      var jobsUri = 'dag';
      var eventsUri = 'events';

      if (baseUri == null) {
        // look for 'localdata' param in current href
        var uri = new URI(window.location.href);
        var params = uri.search(true);
        if (params.localdata) {
          if (params.localdata == 'small') {
            jobsUri = 'data/small-dag.json';
            eventsUri = 'data/small-events.json';
          } else {
            jobsUri = 'data/large-dag.json';
            eventsUri = 'data/large-events.json';
          }
        }
      } else {
        // resolve relative paths given base uri
        var uri = new URI(baseUri);
        jobsUri = new URI(jobsUri).absoluteTo(uri);
        eventsUri = new URI(eventsUri).absoluteTo(uri);
      }

      this.jobsUri = new URI(jobsUri);
      this.eventsUri = new URI(eventsUri);
    },

    /**
     * Submits asynchronous request for workflow jobs from server.
     *
     * @param workflowId id of workflow for which to retrieve jobs.
     * @return a jQuery Promise on which success and error callbacks may be registered.
     */
    getJobs: function(workflowId) {
      var self = this;
      return $.getJSON(new URI(this.jobsUri).addSearch({
        workflowId: workflowId
      }).unicode())
        .error(function(jqXHR, textStatus, errorThrown) {
          console.error('Failed to get jobs:', self, textStatus, errorThrown);
        })
        .success(function(data, textStatus, jqXHR) {
          console.debug('Succeeded to get jobs:', textStatus, data);
        });
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
      var self = this;
      if (lastEventId == null) lastEventId = -1;
      return $.getJSON(new URI(this.eventsUri).addSearch({
        workflowId: workflowId,
        lastEventId: lastEventId,
      }).unicode())
        .error(function(jqXHR, textStatus, errorThrown) {
          console.error('Failed to get events:', self, textStatus, errorThrown);
        })
        .success(function(data, textStatus, jqXHR) {
          console.debug('Succeeded to get events:', textStatus, data);
        });
    },
  };

  // Bind prototype to ctor
  Client.fn.init.prototype = Client.fn;
  return Client;
});
