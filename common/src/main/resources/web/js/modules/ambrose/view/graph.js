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
define(['lib/jquery', 'lib/d3', '../core', './core'], function(
  $, d3, Ambrose, View
) {

  // utility functions
  function isPseudo(node) { return node.pseudo; }
  function isReal(node) { return !(node.pseudo); }
  var progressToAngle = d3.interpolate(0, Math.PI);

  // generates an accessor for the given job state property
  function genJobStateAccessor(property) {
    return function(d) {
      return (d && d.data && d.data.mapReduceJobState && d.data.mapReduceJobState[property])
        ? d.data.mapReduceJobState[property]
        : null;
    };
  }

  // generates a getter for map / reduce progress property
  function genProgressValue(type) {
    return genJobStateAccessor(type + 'Progress');
  }

  // generates a [gs]etter for map / reduce progress angle property
  function genProgressAngle(type) {
    var property = type + 'ProgressAngle';
    return function(d, value) {
      if (value != null) d[property] = value;
      return d[property] || 0.0;
    };
  }

  // generate (map / reduce) progress (value / angle) getters
  var progress = {};
  ['map', 'reduce'].map(function(type) {
    progress[type] = {
      value: genProgressValue(type),
      angle: genProgressAngle(type),
    };
  });

  // Graph ctor
  var Graph = View.Graph = function(workflow, container, params) {
    return new View.Graph.fn.init(workflow, container, params);
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
      self.workflow = workflow;
      self.container = container = $(container);
      self.params = $.extend(true, {
        dimensions: {
          node: {
            radius: 8,
            progress: {
              radius: 12,
            },
            magnitude: {
              radius: {
                min: 12,
                max: 32,
              },
            },
          },
          edge: {
          },
        },
      }, View.Theme, params);
      self.resetView();

      // create arc generators
      var dim = self.params.dimensions;
      self.progressArc = d3.svg.arc()
        .innerRadius(0).outerRadius(dim.node.progress.radius)
        .startAngle(0).endAngle(function(a) { return a; });

      // ensure we resize appropriately
      $(window).resize(function() {
        self.resetView();
        self.handleJobsLoaded();
      });

      // bind event workflow handlers
      workflow.on('jobsLoaded', function() {
        self.handleJobsLoaded();
      });
      workflow.on('jobStarted jobProgress jobComplete jobFailed', function(event, job) {
        self.handleJobsUpdated([job]);
      });
      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        self.handleMouseInteraction($.grep([prev, job], function(j) { return j != null; }));
      });
    },

    resetView: function() {
      // initialize dimensions
      var container = this.container;
      var dim = this.dimensions = {};
      var width = dim.width = container.width();
      var height = dim.height = container.height();

      // create canvas and supporting d3 objects
      this.svg = d3.select(container.empty().get(0))
        .append('svg:svg')
        .attr('class', 'ambrose-view-graph')
        .attr('width', width)
        .attr('height', height);
      var xs = this.xs = d3.scale.linear().range([0, width]);
      var ys = this.ys = d3.scale.linear().range([0, height]);
      this.projection = function(d) { return [xs(d.x), ys(d.y)]; };
    },

    handleJobsLoaded: function() {
      // compute node x,y coords
      var graph = this.workflow.graph;
      var groups = graph.topologicalGroups;
      var groupCount = groups.length;
      var groupDelta = 1 / groupCount;
      var groupOffset = groupDelta / 2;
      $.each(groups, function(i, group) {
        var x = i * groupDelta + groupOffset;

        // determine number of real and pseudo nodes in group
        var realNodes = group.filter(isReal);
        var pseudoNodes = group.filter(isPseudo);
        var realNodeCount = realNodes.length;
        var pseudoNodeCount = pseudoNodes.length;
        var nodeCount = group.length;

        // count number of real and psuedo intervals between nodes
        var realIntervals = 1; // padding
        var pseudoIntervals = 0;
        for (var j = 0; j < group.length - 1; j++) {
          var n1 = group[j];
          var n2 = group[j+1];
          if (isReal(n1) || isReal(n2)) {
            realIntervals++;
          } else {
            pseudoIntervals++;
          }
        }

        // compute real and pseudo intervals
        var pseudoToRealIntervalRatio = 5.0;
        var pseudoIntervalDelta =
          1.0 / (
            (realIntervals * pseudoToRealIntervalRatio) + pseudoIntervals
          );
        var realIntervalDelta = pseudoIntervalDelta * pseudoToRealIntervalRatio;
        var offset = realIntervalDelta / 2.0;

        // assign vertical offsets to nodes; create edges
        $.each(group, function(j, node) {
          node.x = x;
          node.y = offset;
          // TODO(Andy Schlaikjer): Fix to support mixed ordering of real / pseudo nodes
          offset += isReal(node) ? realIntervalDelta : pseudoIntervalDelta;
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

    handleJobsUpdated: function(jobs) {
      var nodes = jobs.map(function(j) { return j.node; });
      this.updateNodeGroups(this.selectNodeGroups(nodes));
    },

    handleMouseInteraction: function(jobs) {
      var nodes = jobs.map(function(j) { return j.node; });
      this.updateNodeGroupsFill(this.selectNodeGroups(nodes));
    },

    selectNodeGroups: function(nodes) {
      return this.svg.selectAll('g.node').data(nodes, function(node) { return node.id; });
    },

    removeNodeGroups: function(g) {
      g.exit().remove();
    },

    createNodeGroups: function(g) {
      var self = this;
      var xs = self.xs;
      var ys = self.ys;
      var projection = self.projection;
      var cx = function(d) { return xs(d.x); };
      var cy = function(d) { return ys(d.y); };

      // create node group elements
      g = g.enter().append('svg:g')
        .attr('class', function(node) {
          return node.pseudo ? 'pseudo' : 'node';
        });

      // create out-bound edges from each node
      g.each(function(node, i) {
        d3.select(this).selectAll('path.edge').data(node.edges).enter()
          .append('svg:path').attr('class', 'edge')
          .attr('d', function(edge, i) {
            var p0 = edge.source,
            p3 = edge.target,
            m = (p0.x + p3.x) / 2,
            p = [p0, {x: m, y: p0.y}, {x: m, y: p3.y}, p3],
            p = p.map(projection);
            return "M" + p[0] + "C" + p[1] + " " + p[2] + " " + p[3];
          });
      });

      // filter down to real nodes
      var real = g.filter(isReal);

      // create translucent circle depicting relative size of each node

      // create arcs depicting MR task completion
      var progress = real.append('svg:g')
        .attr('class', 'progress')
        .attr('transform', function(d) {
          return 'translate(' + xs(d.x) + ',' + ys(d.y) + ') rotate(-90)';
        });
      progress.append('svg:path')
        .attr('class', 'progress map');
      progress.append('svg:path')
        .attr('class', 'progress reduce');

      // create smaller circle and bind event handlers
      real.append('svg:circle')
        .attr('class', 'anchor')
        .attr('cx', cx)
        .attr('cy', cy)
        .attr('r', self.params.dimensions.node.radius)
        .on('mouseover', function(node) { self.workflow.mouseOverJob(node.data); })
        .on('mouseout', function(node) { self.workflow.mouseOverJob(null); })
        .on('click', function(node) { self.workflow.selectJob(node.data); });
    },

    updateNodeGroups: function(g) {
      var self = this;
      var duration = 750;

      function getArcTween(progress, scale) {
        return function(d) {
          var b = progress.angle(d);
          var e = progress.angle(d, progressToAngle(scale * progress.value(d)));
          var i = d3.interpolate(b, e);
          return function(t) {
            return self.progressArc(i(t));
          };
        };
      }

      self.updateNodeGroupsFill(g, duration);
      g.selectAll('g.node path.map').transition().duration(duration).attrTween("d", getArcTween(progress.map, 1.0));
      g.selectAll('g.node path.reduce').transition().duration(duration).attrTween("d", getArcTween(progress.reduce, -1.0));
    },

    updateNodeGroupsFill: function(g, duration) {
      var self = this;
      var colors = self.params.colors;

      function fill(node) {
        var job = node.data;
        var status = job.status || '';
        if (job.mouseover) return colors.mouseover;
        if (job.selected) return colors.selected;
        return colors[status.toLowerCase()] || '#555';
      }

      var s = g.selectAll('g.node circle.anchor');
      if (duration > 0) s = s.transition().duration(duration);
      s.attr('fill', fill);
    },
  };

  // bind prototype to ctor
  Graph.fn.init.prototype = Graph.fn;
  return Graph;
});
