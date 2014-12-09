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
  // GraphNodePopover ctor
  var GraphNodePopover = View.GraphNodePopover = function(workflow, graphView) {
    return new View.GraphNodePopover.fn.init(workflow, graphView);
  };

  /**
   * GraphNodePopover prototype.
   */
  GraphNodePopover.fn = GraphNodePopover.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param graphContainer the container for all the graph elements
     */
    init: function(workflow, graphView) {
      var self = this;
      self.workflow = workflow;
      self.graphView = graphView;
      self.container = graphView.container;

      // create node popovers once graph view is initialized
      workflow.on('graph.view.initialized', function(event, jobs) {
        if (jobs && jobs.length > 0 && jobs[0].runtime == 'pig') {
          self.createNodePopovers();
        }
      });

      // show node popover when a job is selected
      workflow.on('jobSelected', function(event, job, prev) {
        self.show(job);
      });
    },

    getNodeTriggers: function() {
      var self = this;
      var nodes = self.container.find('circle.trigger');
      return nodes;
    },

    show: function(job) {
      var self = this;
      if (job != null) {
        // find anchor for currently selected job
        var current = self.container.find('[data-node-id="' + job.node.id + '"]')

        // hide all other popovers
        self.getNodeTriggers().not(current).popover('hide');

        // show current popover
        current.popover('show');
      } else {
        // hide all
        self.getNodeTriggers().popover('hide');
      }
    },

    /**
     * Creates graph view node popovers.
     */
    createNodePopovers: function() {
      var self = this;

      function getTitle(node) {
        var $title = $('<div class="ambrose-view-graph-popover-title">');
        if (node.__data__.data.mapReduceJobState) {
          var mrJobState = node.__data__.data.mapReduceJobState;
          $('<a>', {
            'target': '_blank',
            'href': mrJobState.trackingURL,
            'text': mrJobState.jobId,
          }).appendTo($title);
        } else {
          $title.text('Job id undefined');
        }
        $('<button type="button" class="close"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>')
          .appendTo($title)
          .click(function() { self.workflow.selectJob(null); });
        return $title;
      }

      function getContent(node) {
        // Create the popover body section based on the node.
        var data = node.__data__.data;
        if (!data) return 'Job details unavailable';

        var body = $('<div class="ambrose-view-graph-popover-body">');
        var list = $('<dl>').appendTo(body);

        function addItem(name, value) {
          $('<dt>').appendTo(list).text(name);
          $('<dd>').appendTo(list).text(value);
        }

        if (data.status) addItem('Status', data.status);
        if (data.aliases) addItem('Aliases', data.aliases.join(', '));
        if (data.features) addItem('Features', data.features.join(', '));

        if (data.mapReduceJobState) {
          var mrJobState = data.mapReduceJobState;

          if (mrJobState.jobStartTime && mrJobState.jobLastUpdateTime) {
            var startTime = mrJobState.jobStartTime;
            var lastUpdateTime = mrJobState.jobLastUpdateTime;
            addItem('Duration', Ambrose.calculateElapsedTime(startTime, lastUpdateTime));
          }

          if (mrJobState.totalMappers) addItem('Mappers', mrJobState.totalMappers);
          if (mrJobState.totalReducers) addItem('Reducers', mrJobState.totalReducers);
        }

        return body;
      }

      // create popovers
      self.getNodeTriggers().each(function (i, node) {
        $(node).popover({
          title: function() { return getTitle(node, i); },
          content: function() { return getContent(node); },
          html: 'true',
          container: 'body',
          placement: 'auto left',
          trigger: 'manual',
        });
      });
    }
  };

  // bind prototype to ctor
  GraphNodePopover.fn.init.prototype = GraphNodePopover.fn;
  return GraphNodePopover;
});
