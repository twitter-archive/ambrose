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
 * This module defines the Edge-Popover view which generates a popover for each edge.
 * This popover only shows when the user is mouseovered the edge.
 */
define(['lib/jquery', '../core', './core', '../job-data'], function($, Ambrose, View, JobData) {
  // GraphEdgePopover ctor
  var GraphEdgePopover = View.GraphEdgePopover = function(workflow, graphView) {
    return new View.GraphEdgePopover.fn.init(workflow, graphView);
  };

  /**
   * GraphEdgePopover prototype.
   */
  GraphEdgePopover.fn = GraphEdgePopover.prototype = {
    /**
     * Constructor.
     *
     * @param workflow Workflow instance to bind to.
     * @param graphView graph view.
     */
    init: function(workflow, graphView) {
      var self = this;
      var navBar = $('.nav.pull-right');
      self.workflow = workflow;
      self.graphView = graphView;
      self.container = graphView.container;
      self.selectedEdge = null;

      // create edge popovers once graph view is initialized
      workflow.on('graph.view.initialized', function(event, jobs) {
        // TODO: Support additional runtimes
        if (jobs && jobs.length > 0 && jobs[0].runtime == 'pig') {
          self.createEdgePopoverForPig(self.container);
        }
      });
    },

    /**
     * Edge popover for pig scripts.
     */
    createEdgePopoverForPig: function(graphContainer) {
      var self = this;

      function getContent(edgeObj) {
        // Create the popover body section based on the node.
        var edge = edgeObj.__data__;
        var sourceData = edge.source.pseudo ? edge.source.source.data : edge.source.data;
        var targetData = edge.target.pseudo ? edge.target.target.data : edge.target.data;
        var defaultContent = 'Counter information not available';

        if (!sourceData.counterGroupMap && !targetData.counterGroupMap) {
          return defaultContent;
        }

        var body = $('<div class="ambrose-view-graph-popover-body">');
        var list = $('<dl>').appendTo(body);
        var itemCount = 0;

        function addItem(name, value) {
          $('<dt>').appendTo(list).text(name);
          $('<dd>').appendTo(list).text(value);
          itemCount++;
        }

        if (sourceData) {
          var hdfsBytesWritten = JobData.getHdfsBytesWritten(sourceData);
          if (hdfsBytesWritten != null) addItem('HDFS Bytes Written', hdfsBytesWritten.commafy());
          var reduceOutputRecords = JobData.getReduceOutputRecords(sourceData);
          if (reduceOutputRecords != null) addItem('Reduce Output Records', reduceOutputRecords.commafy());
        }

        if (targetData) {
          var mapInputRecords = JobData.getMapInputRecords(targetData);
          if (mapInputRecords != null) addItem('Map Input Records', mapInputRecords.commafy());
          var hdfsBytesRead = JobData.getHdfsBytesRead(targetData);
          if (hdfsBytesRead != null) addItem('HDFS Bytes Read', hdfsBytesRead.commafy());
        }

        if (itemCount == 0) return defaultContent;
        return body;
      }

      graphContainer.find('path.trigger').each(function(i, edge) {
        var $edge = $(edge);
        var $title;
        $edge.popover({
          // Set the placement to "top", so width of the popover can be accurately measured.
          placement: 'top',
          container: 'body',
          html: 'true',
          title: function () {
            $title = $('<div class="ambrose-view-graph-popover-title">').text('Counters');
            // TODO: Fix edge selection state, events, and close button
            $('<button type="button" class="close"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>')
              .appendTo($title)
              .click(function() {
                $edge.popover('hide');
                self.selectedEdge = null;
              });
            return $title;
          },
          content: function() {
            return getContent(edge);
          },
          trigger: 'manual'
        }).click(function(e) {
          // Since the edge takes up a rectangular space and Twitter Popover can't find the
          // center of the section to place the popover, we will handle the trigger manually.

          // Hide all other edge popover.
          graphContainer.find('path.trigger').not(edge).popover('hide');

          // If the previous selected edge is the same one, hide popover and return.
          if (self.selectedEdge === edge) {
            $(self.selectedEdge).popover('hide');
            self.selectedEdge = null;
            return;
          }

          // Show the new popover and update the current selected edge to this edge.
          $edge.popover('show');
          self.selectedEdge = edge;

          // After the popover is display, select the title of the popover, then get the whole
          // popover of the edge (parent's parent of the title).
          var popover = $title;
          if (popover && popover.parent() && popover.parent().parent()) {
            popover = popover.parent().parent();
          }

          // Find the top/left position of the edge and height/width of the edge area.
          var edgeAreaTop = $edge.offset().top;
          var edgeAreaLeft = $edge.offset().left;
          var targetHeight = e.target.getBoundingClientRect().height;
          var targetWidth = e.target.getBoundingClientRect().width;

          // Place the popover at the center of the div.
          var top = edgeAreaTop + (targetHeight/2) - popover.height();
          // -50 px to count the height of the nab bar, check if enough space to place it on top.
          if (e.target.getBoundingClientRect().top + (targetHeight/2) -50 < popover.height()) {
            popover.removeClass('top');
            popover.addClass('bottom');
            top = edgeAreaTop + (targetHeight/2);
          }

          popover.css({
            top: top,
            left: edgeAreaLeft + (targetWidth/2) - (popover.width()/2)
          });
        });
      });
    }
  };

  // bind prototype to ctor
  GraphEdgePopover.fn.init.prototype = GraphEdgePopover.fn;
  return GraphEdgePopover;
});
