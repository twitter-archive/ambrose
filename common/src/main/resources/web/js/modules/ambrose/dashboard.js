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
    'RUNNING',
    'SUCCEEDED',
    'FAILED'
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
          .text(id.capitalize())
          .click(function() { self.setStatus(id); self.loadFlows(); });
      });

      // configure event handlers
      $('#user-form').submit(function() {
        self.setUser($('#user-field').val()); self.loadFlows(); return false;
      });
      $('#page-prev-link').click(function() { self.prevPage(); });
      $('#page-next-link').click(function() { self.nextPage(); });

      // set default values
      self.setStatus('RUNNING');
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
      return this;
    },

    setStatus: function(status) {
      this.status = status;
      $('.status').removeClass('active');
      $('#status_' + status).addClass('active');
      return this;
    },

    setUser: function(user) {
      this.user = user;
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
      self.client.getWorkflows(self.clusters[self.cluster], self.user, self.status, self.currentStartKey)
        .success(function(data) {
          self.nextStartKey = data.nextPageStart;
          self.renderFlows(data);
        });
      return this;
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
        $('<td>').text(workflow.name).appendTo($tr);
        $('<td>').text(workflow.status).appendTo($tr);
        $('<div class="bar">').width(workflow.progress + '%').appendTo(
          $('<div class="progress">').appendTo($('<td>').appendTo($tr)));
      });
      if (self.nextStartKey != null && self.nextStartKey != '') {
        $('#page-next-link').removeClass('disabled');
      } else {
        $('#page-next-link').addClass('disabled');
      }
      if (self.prevStartKeys.length > 0) {
        $('#page-next-link').removeClass('disabled');
      } else {
        $('#page-next-link').addClass('disabled');
      }
      return this;
    },
  };

  // bind prototype to ctor
  Dashboard.fn.init.prototype = Dashboard.fn;
  return Dashboard;
});
