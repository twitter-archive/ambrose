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
 * This module defines the Node-Popover view which generates a popover for each job node.
 * The popover toggles on click.
 */
define(['lib/jquery', '../core', './core'], function($, Ambrose, View) {
  // NodePopover ctor
  var NodePopover = View.NodePopover = function(workflow, container) {
    return new View.NodePopover.fn.init(workflow, container);
  };

  /**
   * NodePopover prototype.
   */
  NodePopover.fn = NodePopover.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param graphContainer the container for all the graph elements
     */
    init: function(workflow, graphContainer) {
      var self = this;
      self.container = graphContainer;

      // Create node popover after the dag is created.
      workflow.on('dagCreated', function(event, jobs) {
        if (jobs && jobs.length > 0 && jobs[0].runtime == 'pig') {
          graphContainer.find('.node circle.anchor').css("cursor", "pointer");
          self.createNodePopoverForPig(graphContainer);
        }
      });
    },

    /**
     * Create the node popover for a pig script.
     */
    createNodePopoverForPig: function(graphContainer) {
      function getPlacment(source) {
        // Place the popover on the left if there is enough space.
        var position = $(source).position();
        if (position.left > 300) { return "left"; }
        return "right";
      }

      function getTitle(node) {
        if (node.__data__.data.mapReduceJobState) {
          var mrJobState = node.__data__.data.mapReduceJobState;
          return $('<a>', { 'target': '_blank', 'href': mrJobState.trackingURL, 'text': mrJobState.jobId });
        } else {
          return $('<span>', { 'class': 'popoverTitle', 'text': 'Job id undefined'});
        }
      }

      function getContent(node) {
        // Create the popover body section based on the node.
        if (!node.__data__.data) {
          return $('<span>', {
            'class': 'popoverTitle', 'style': 'margin-left:10px', 'text': 'Job details unavailable'});
        }

        var data = node.__data__.data;
        var bodyEL = $('<div>', { 'class': 'popoverBody'});
        var jobInfoList = $('<ul>').appendTo(bodyEL);

        if (data.status) {
          var item = $('<li>').appendTo(jobInfoList);
          $('<span>', { 'class': 'popoverKey', 'text': 'Status: '}).appendTo(item);
          $('<span>', { 'text': data.status }).appendTo(item);
        }

        if (data.aliases) {
          var item = $('<li>').appendTo(jobInfoList);
          $('<span>', { 'class': 'popoverKey', 'text': 'Aliases: '}).appendTo(item);
          $('<span>', { 'text': data.aliases.join(', ') }).appendTo(item);
        }

        if (data.features) {
          var item = $('<li>').appendTo(jobInfoList);
          $('<span>', { 'class': 'popoverKey', 'text': 'Features: '}).appendTo(item);
          $('<span>', { 'text': data.features.join(', ') }).appendTo(item);
        }

        if (data.mapReduceJobState) {
          var mrJobState = data.mapReduceJobState;
          if (mrJobState.jobStartTime && mrJobState.jobLastUpdateTime) {
            var startTime = mrJobState.jobStartTime;
            var lastUpdateTime = mrJobState.jobLastUpdateTime;

            var item = $('<li>').appendTo(jobInfoList);
            $('<span>', { 'class': 'popoverKey', 'text' : 'Duration: '}).appendTo(item);
            $('<span>', { 'text': Ambrose.calculateElapsedTime(startTime, lastUpdateTime) }).appendTo(item);
          }

          if (mrJobState.totalMappers) {
            var item = $('<li>').appendTo(jobInfoList);
            $('<span>', { 'class': 'popoverKey', 'text' : 'Mappers: '}).appendTo(item);
            $('<span>', { 'text': mrJobState.totalMappers }).appendTo(item);
          }

          if (mrJobState.totalReducers) {
            var item = $('<li>').appendTo(jobInfoList);
            $('<span>', { 'class': 'popoverKey', 'text' : 'Reducers: '}).appendTo(item);
            $('<span>', { 'text': mrJobState.totalReducers }).appendTo(item);
          }
        }
        return bodyEL;
      }

      // Display Popover.
      graphContainer.find(".node circle.anchor").each(function (i, node) {
        $(this).popover({
          placement : function (context, source) { return getPlacment(source); },
          title : function (){ return getTitle(this) },
          content: function (){ return getContent(this); },
          container : 'body',
          html : 'true'
        });
      });

      graphContainer.find('.node circle.anchor').click(function(e) {
        // Hide all popover but the one just clicked.
        graphContainer.find('.node circle.anchor').not(this).popover('hide');
      });
    }
  };

  // bind prototype to ctor
  NodePopover.fn.init.prototype = NodePopover.fn;
  return NodePopover;
});