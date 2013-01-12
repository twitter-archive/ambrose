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
 * This module defines the Graph view which generates horizontal DAG view of Workflow jobs.
 */
define(['jquery', 'd3', 'colorbrewer', '../core', './core'], function(
  $, d3, colorbrewer, Ambrose, Views
) {
  // utility functions
  function isPseudo(node) { return node.pseudo; }
  function isReal(node) { return !(node.pseudo); };

  // Graph ctor
  var Graph = Views.Graph = function(workflow, container, params) {
    return new Views.Graph.fn.init(workflow, container, params);
  }

  /**
   * Graph prototype.
   */
  Graph.fn = Graph.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param container the DOM element in which to render the view.
     * @param params extra parameters.
     */
    init: function(workflow, container, params) {
      var self = this;
      this.workflow = workflow;
      this.container = container = $(container);

      // define default params and override with user supplied params
      var params = this.params = $.extend(true, {
        colors: {
          running: d3.rgb(98, 196, 98).brighter(),
          complete: d3.rgb(98, 196, 98),
          failed: d3.rgb(196, 98, 98),
          mouseover: d3.rgb(98, 98, 196).brighter(),
          selected: d3.rgb(98, 98, 196),
        },
        palettes: {
          queued: colorbrewer.Greys,
          complete: colorbrewer.Greens,
          failed: colorbrewer.Reds,
        },
        dimensions: {
          padding: 20,
        },
      }, params);

      this.resetView();

      // bind event workflow handlers
      workflow.on('jobsLoaded', function(event, jobs) {
        self.handleJobsLoaded(jobs);
      });
      workflow.on('jobStarted jobProgress jobCompleted jobFailed', function(event, job) {
        self.handleJobsUpdated([job], 350);
      });
      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        self.handleJobsUpdated($.grep([prev, job], Ambrose.notNull));
      });
    },

    resetView: function() {
      // initialize dimensions
      var container = this.container;
      var dim = this.dimensions = {};
      var width = dim.width = container.width();
      var height = dim.height = container.height();
      var padding = this.params.dimensions.padding;

      // create canvas and supporting d3 objects
      this.svg = d3.select(container.empty().get(0))
        .append('svg:svg')
        .attr('class', 'ambrose-views-graph')
        .attr('width', width)
        .attr('height', height);
      var xs = this.xs = d3.scale.linear().range([0, width]);
      var ys = this.ys = d3.scale.linear().range([0, height]);
      this.projection = function(d) { return [xs(d.x), ys(d.y)]; };
    },

    handleJobsLoaded: function(jobs) {
      // compute node x,y coords
      var graph = this.workflow.graph;
      var groups = graph.topologicalGroups;
      var groupCount = groups.length;
      var groupDelta = 1 / groupCount;
      var groupOffset = groupDelta / 2;
      // var edges = this.edges = [];
      $.each(groups, function(i, group) {
        var x = i * groupDelta + groupOffset;
        var nodeCount = group.length;
        var nodeDelta = 1 / nodeCount;
        var nodeOffset = nodeDelta / 2;
        $.each(group, function(j, node) {
          node.x = x;
          node.y = j * nodeDelta + nodeOffset;
          var edges = node.edges = [];
          $.each(node.parents || [], function(p, parent) {
            edges.push({ source: node, target: parent });
          });
        });
      });

      var graph = this.workflow.graph;
      var nodes = graph.nodes.concat(graph.pseudoNodes).sort(function(a, b) {
        return b.topologicalGroupIndex - a.topologicalGroupIndex;
      });
      var g = this.selectNodeGroups(nodes);
      this.removeNodeGroups(g);
      this.createNodeGroups(g);
      this.updateNodeGroups(g);
    },

    handleJobsUpdated: function(jobs, duration) {
      var nodes = $.map(jobs, function(job) { return job.node; });
      this.updateNodeGroups(this.selectNodeGroups(nodes), duration);
    },

    selectNodeGroups: function(nodes) {
      return this.svg.selectAll('g.node').data(nodes, function(node) { return node.id; });
    },

    removeNodeGroups: function(g) {
      g.exit().remove();
    },

    createNodeGroups: function(g) {
      var self = this;
      var xs = this.xs;
      var ys = this.ys;
      var projection = this.projection;
      g = g.enter().append('svg:g').attr('class', function(node) {
        return node.pseudo ? 'pseudo' : 'node';
      });
      g.each(function(node, i) {
        d3.select(this).selectAll('path.edge').data(node.edges).enter()
          .append('svg:path').attr('class', 'edge')
          .attr('d', function diagonal(edge, i) {
            var p0 = edge.source,
            p3 = edge.target,
            m = (p0.x + p3.x) / 2,
            p = [p0, {x: m, y: p0.y}, {x: m, y: p3.y}, p3],
            p = p.map(projection);
            return "M" + p[0] + "C" + p[1] + " " + p[2] + " " + p[3];
          });
      });
      var c = g.filter(isReal).append('svg:circle')
        .attr('cx', function(d) { return xs(d.x); })
        .attr('cy', function(d) { return ys(d.y); })
        .attr('r', 8)
        .on('mouseover', function(node) { self.workflow.mouseOverJob(node.data); })
        .on('mouseout', function(node) { self.workflow.mouseOverJob(null); })
        .on('click', function(node) { self.workflow.selectJob(node.data); });
    },

    updateNodeGroups: function(g, duration) {
      var colors = this.params.colors;
      var fill = function(node) {
        var job = node.data;
        var status = job.status;
        if (job.mouseover) return colors.mouseover;
        if (job.selected) return colors.selected;
        if (status == 'RUNNING') return colors.running;
        if (status == 'COMPLETE') return colors.complete;
        if (status == 'FAILED') return colors.failed;
        return '#555';
      };
      g.selectAll('g.node circle').attr('fill', fill);
    },
  };

  // bind prototype to ctor
  Graph.fn.init.prototype = Graph.fn;
  return Graph;
});
