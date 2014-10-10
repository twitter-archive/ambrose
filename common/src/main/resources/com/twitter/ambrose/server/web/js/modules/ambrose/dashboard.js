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
 * Ambrose dashboard module.
 */
define(['lib/jquery', './core', './client'], function($, Ambrose, Client) {
  var statusSet = [
    'running',
    'succeeded',
    'failed'
  ];

  // Dashboard ctor
  var Dashboard = Ambrose.Dashboard = function(client) {
    return new Ambrose.Dashboard.fn.init(client);
  };

  /**
   * Dashboard prototype.
   */
  Dashboard.fn = Dashboard.prototype = {
    /**
     * Constructs a new Dashboard.
     *
     * @param client client to use when requesting workflow summaries from server. If null, defaults
     * to new client instance constructed with default arguments.
     */
    init: function(client) {
      console.log('Initializing Ambrose Dashboard');
      var self = this;
      self.client = client || Client();
      self.currentStartKey = '';
      self.nextStartKey = '';
      self.prevStartKeys = [];

      // build status menu
      $.each(statusSet, function(index, id) {
        $('<a>').appendTo(
          $('<li>').appendTo($('#status-menu'))
            .attr('id', 'status_' + id).addClass('status'))
          .text(id)
          .click(function() {
            // Reset the keys and status when clicked on a different status.
            self.currentStartKey = '';
            self.nextStartKey = '';
            self.prevStartKeys = [];
            self.setStatus(id);

            // Get the workflows
            self.loadFlows();
          });
      });

      // configure event handlers
      $('#user-form').submit(function() {
        self.setUser($('#user-field').val()); self.loadFlows(); return false;
      });
      $('#page-prev-link').click(function() {
        if (!$('#page-prev-link').hasClass("disabled")) { self.prevPage(); }
      });
      $('#page-next-link').click(function() {
        if (!$('#page-next-link').hasClass("disabled")) { self.nextPage(); }
      });

      // set default values
      self.setStatus('running');
      self.setUser('');

      // request clusters
      self.client.getClusters()
        .fail(function() {
          $('#cluster-dropdown').popover({
            placement: 'bottom',
            trigger: 'manual',
            title: 'Warning',
            content: 'Failed to retrieve clusters from backend.',
            container: 'body',
          }).popover('show').addClass('text-error');
          self.initClusters({ undefined: 'undefined' });
        })
        .done(function(clusters) {
          self.initClusters(clusters);
          self.loadFlows();
        }).fail();
    },

    getStatusSet: function() {
      return statusSet;
    },

    initClusters: function(clusters) {
      self = this;
      self.clusters = clusters;
      var defaultCluster;

      // build clusters menu
      $.each(clusters, function(id, name) {
        if (!defaultCluster) defaultCluster = id;
        $('<a>').appendTo(
          $('<li>').appendTo($('#cluster-menu'))
            .attr('id', 'cluster_' + id).addClass('cluster'))
          .text(name)
          .click(function() { self.setCluster(id); self.loadFlows(); });
      });

      // set default cluster
      self.setCluster(defaultCluster);
    },

    setCluster: function(cluster) {
      this.cluster = cluster;
      $('.cluster').removeClass('active');
      $('#cluster_' + cluster).addClass('active');
      $('#current-cluster').text(cluster);
      return this;
    },

    setStatus: function(status) {
      this.status = status;
      $('.status').removeClass('active');
      $('#status_' + status).addClass('active');
      $('#current-status').text(status);
      return this;
    },

    setUser: function(user) {
      this.user = user;
      $('#current-user').text(user || 'any');
      return this;
    },

    prevPage: function() {
      this.currentStartKey = this.prevStartKeys.pop();
      this.loadFlows();
      return this;
    },

    nextPage: function() {
      this.prevStartKeys.push(this.currentStartKey);
      this.currentStartKey = this.nextStartKey;
      this.loadFlows();
      return this;
    },

    loadFlows: function() {
      var self = this;
      var cluster = self.cluster;
      var user = self.user;
      var status = self.status;
      status = status.toUpperCase();
      self.client.getWorkflows(self.clusters[cluster], user, status, self.currentStartKey)
        .success(function(data) {
          self.nextStartKey = data.nextPageStart;
          self.renderFlows(data);
        });
      return self;
    },

    renderFlows: function(data) {
      var self = this;
      var workflows = data.results;
      var $workflows = $('#workflows').empty();
      var pageOffset = self.prevStartKeys.length * 10;
      $.each(workflows, function(i, workflow) {
        var $tr = $('<tr>').appendTo($workflows).attr('id', 'workflow_' + workflow.id)
          .click(function() {
            window.location.href = 'workflow.html?workflowId=' + encodeURIComponent(workflow.id);
          });
        $('<td>').text(pageOffset + i + 1).appendTo($tr);
        $('<td>').text(workflow.userId).appendTo($tr);
        var createdAt = workflow.createdAt ? workflow.createdAt.formatTimestamp() : 'unknown';
        $('<td>').text(createdAt).appendTo($tr);
        $('<td>').text(workflow.name).appendTo($tr);
        $('<td>').text(workflow.status.toLowerCase()).appendTo($tr);
        $('<div class="bar">').width(workflow.progress + '%').appendTo(
          $('<div class="progress">').appendTo($('<td>').appendTo($tr)));
      });
      if (self.nextStartKey != null && self.nextStartKey != '') {
        $('#page-next-link').removeClass('disabled');
      } else {
        $('#page-next-link').addClass('disabled');
      }
      if (self.prevStartKeys.length > 0) {
        $('#page-prev-link').removeClass('disabled');
      } else {
        $('#page-prev-link').addClass('disabled');
      }
      return this;
    },
  };

  // bind prototype to ctor
  Dashboard.fn.init.prototype = Dashboard.fn;
  return Dashboard;
});
