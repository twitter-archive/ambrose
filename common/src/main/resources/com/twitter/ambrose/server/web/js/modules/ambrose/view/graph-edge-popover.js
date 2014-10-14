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
  // EdgePopover ctor
  var EdgePopover = View.EdgePopover = function(workflow, container, graph) {
    return new View.EdgePopover.fn.init(workflow, container, graph);
  };

  /**
   * EdgePopover prototype.
   */
  EdgePopover.fn = EdgePopover.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param graphContainer the container for all the graph elements
     * @param the graph view
     */
    init: function(workflow, graphContainer, graph) {
      var self = this;
      var navBar = $('.nav.pull-right');
      self.container = graphContainer;
      self.selectedEdge = null;

      // Create edge popover after the dag is created.
      workflow.on('dagCreated', function(event, jobs) {
        if (jobs && jobs.length > 0 && jobs[0].runtime == 'pig') {
          // Create settings nav bar only if the job is a pig job.
          // This will change if more setting items are added.
          if ($('#ambrose-navbar .settingsDropdown a').length == 0) {
            self.createSettingsDropdown(navBar);
            self.addSettingsClickEvents(graph, navBar, workflow);
          }
          self.createEdgePopoverForPig(graphContainer);
        }
      });
    },

    /**
     * Generate the Settings dropdown on demand.
     */
    createSettingsDropdown: function(navBar) {
      var dropDownList = $('<li class="dropdown settingsDropdown">');
      navBar.prepend(dropDownList);
      var dropdownToggleEL = $('<a>', { 'class': 'dropdown-toggle', 'ref' : '#', 'data-toggle': 'dropdown'}).appendTo(dropDownList);
      $('<span>', {'text': 'Settings'}).appendTo(dropdownToggleEL);
      $('<b>', {'class': 'caret'}).appendTo(dropdownToggleEL);

      var dropDownMenu = $('<ul>', {
        'class': 'dropdown-menu',
        'id' : 'settingsDropdown',
        'role': 'menu',
        'aria-labelledby':'settingsDropdown'
      }).appendTo(dropDownList);

      // Add the all the dropdown items.
      dropDownMenu.html(' <li role="presentation" class="dropdown-header">Edge Scaled By</li>'
          + '<li><a href="#" class="edgeScaleOption" id="noEdgeScaling">No Edge Scaling</a></li>'
          + '<li><a href="#" class="edgeScaleOption" id="hdfsBytesWritten">Source HDFS Bytes Written</a></li>'
          + '<li><a href="#" class="edgeScaleOption" id="reduceOutputRecords">Source Reduce Output Records</a></li>'
          + '<li><a href="#" class="edgeScaleOption" id="mapInputRecords">Destination Map Input Records</a></li>'
          + '<li><a href="#" class="edgeScaleOption" id="hdfsBytesRead">Destination HDFS Bytes Read</a></li>');
    },

    /**
     * Add click events to settings dropdown.
     */
    addSettingsClickEvents : function(graph, navBar, workflow) {
      // Grey out not selected scaling options, and put a checkmark to mark the selected option.
      function selectEdgeOption() {
        navBar.find(".edgeScaleOption").toggleClass("greyText", true);
        navBar.find(".edgeScaleOption .icon-ok").remove();
        navBar.find("#" + workflow.rescaleOption).toggleClass("greyText", false);
        navBar.find("#" + workflow.rescaleOption).prepend('<i class="icon-ok"></i>');
      }

      // When the popover is created successfully, add click events to it.
      selectEdgeOption();
      navBar.find(".edgeScaleOption").each(function () {
        $(this).click(function(){
          workflow.rescaleOption = this.id;
          selectEdgeOption();

          graph.arcValueMax = 0;
          graph.arcValueMin = 0;
          graph.rescaleEdges();
        });
      });
    },


    /**
     * Edge popover for pig scripts.
     */
    createEdgePopoverForPig: function(graphContainer) {
      var self = this;
      var $self = $(self);

      function getContent(edgeObj) {
        // Create the popover body section based on the node.
        var edge = edgeObj.__data__;
        var targetData = null;
        var sourceData = null;
        if (edge) {
          targetData = edge.target.data;
          if (edge.target.pseudo) { targetData =  edge.target.targetData; }
          sourceData = edge.source.data;
          if (edge.source.pseudo) { sourceData =  edge.source.sourceData; }
        }

        var defaultContent = $('<div class="ambrose-view-graph-edge-popover-default">')
          .text('Counter information not available');

        if (!targetData.counterGroupMap && !sourceData.counterGroupMap) {
          return defaultContent;
        }

        var bodyEL = $('<div class="popoverBody">');
        var counterList = $('<ul>');

        // target node counters
        if (targetData) {
          var $targetData = $('<ul class="counterList">');

          if (JobData.getHDFSWrittenFromCounter(targetData)) {
            var item = $('<li>').appendTo($targetData);
            $('<span>', { 'class': 'popoverKey', 'text' : 'HDFS Bytes Written: '}).appendTo(item);
            $('<span>', { 'text': JobData.getHDFSWrittenFromCounter(targetData).commafy() }).appendTo(item);
          }

          if (JobData.getReduceOutputRecords(targetData)) {
            var item = $('<li>').appendTo($targetData);
            $('<span>', { 'class': 'popoverKey', 'text': 'Reduce Output Records: '} ).appendTo(item);
            $('<span>', { 'text': JobData.getReduceOutputRecords(targetData).commafy() }).appendTo(item);
          }

          if ($targetData.find('li').length > 0) {
            $targetData.prepend($('<li>', { 'class': 'counterListTitle', 'text': 'Source Node:' }));
            $targetData.appendTo(counterList);
          }
        }

        // Source Node Counters
        if (sourceData) {
          var $sourceData = $('<ul class="counterList">');

          if (JobData.getMapInputRecords(sourceData)) {
            var item = $('<li>').appendTo($sourceData);
            $('<span>', { 'class': 'popoverKey', 'text' : 'Map Input Records: '} ).appendTo(item);
            $('<span>', { 'text': JobData.getMapInputRecords(sourceData).commafy() }).appendTo(item);
          }

          if (JobData.getHDFSReadFromCounter(sourceData)) {
            var item = $('<li>').appendTo($sourceData);
            $('<span>', { 'class': 'popoverKey', 'text' : 'HDFS Bytes Read: '} ).appendTo(item);
            $('<span>', { 'text': JobData.getHDFSReadFromCounter(sourceData).commafy() }).appendTo(item);
          }

          if ($sourceData.find('li').length > 0) {
            $sourceData.prepend($('<li>', { 'class': 'counterListTitle', 'text': 'Destination Node:' }));
            $sourceData.appendTo(counterList);
          }
        }

        if (counterList.find('.counterList').length > 0) {
          counterList.appendTo(bodyEL);
          return bodyEL;
        }

        return defaultContent;
      }

      graphContainer.find('path.pseudoEdge').each(function(i, edge) {
        var edge = this;
        var $edge = $(edge);
        var $title;
        $edge.popover({
          // Set the placement to "top", so width of the popover can be accurately measured.
          placement: 'top',
          container : 'body',
          html: 'true',
          title: function () {
            $title = $('<div class="ambrose-view-graph-popover-title">').text('Counters');
            var $close = $('<button class="close">').html('&times;')
              .appendTo($title)
              .click(function() {
                $edge.popover('hide');
                self.selectedEdge = null;
              });
            return $title;
          },
          content : function() {
            return getContent(edge);
          },
          trigger: 'manual'
        }).click(function(e) {
          // Since the edge takes up a rectangular space and Twitter Popover can't find the
          // center of the section to place the popover, we will handle the trigger manually.

          // Hide all other edge popover.
          graphContainer.find('path.pseudoEdge').not(edge).popover('hide');

          // Find the top/left position of the edge and height/width of the edge area.
          var edgeAreaTop = $edge.offset().top;
          var edgeAreaLeft = $edge.offset().left;
          var targetHeight = e.target.getBoundingClientRect().height;
          var targetWidth = e.target.getBoundingClientRect().width;

          // If the previous selected edge is the same one, hide popover and return.
          if (self.selectedEdge == edge) {
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

          // Place the popover at the center of the div.
          var top = edgeAreaTop + (targetHeight/2) - popover.height();
          // -50 px to count the height of the nab bar, check if enough space to place it on top.
          if (e.target.getBoundingClientRect().top + (targetHeight/2) -50 < popover.height()) {
            popover.toggleClass("top", false);
            popover.toggleClass("bottom", true);
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
  EdgePopover.fn.init.prototype = EdgePopover.fn;
  return EdgePopover;
});
