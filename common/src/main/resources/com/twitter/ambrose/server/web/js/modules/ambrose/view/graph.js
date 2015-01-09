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
define([
  'lib/jquery',
  'lib/underscore',
  'lib/uri',
  'lib/d3',
  '../core',
  './core',
  '../job-data'
], function(
  $, _, URI, d3, Ambrose, View, JobData
) {
  // utility functions
  function isPseudo(node) { return node.pseudo; }
  function isReal(node) { return !(node.pseudo); }
  var progressToAngle = d3.interpolate(0, 2.0 * Math.PI);

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

      var nodeRadius = 8;

      self.params = $.extend(true, View.Theme, {
        dimensions: {
          node: {
            radius: nodeRadius,
            progress: {
              map: { radius: nodeRadius + 4 },
              reduce: { radius: nodeRadius + 8 },
            },
            metric: {
              radius: {
                min: nodeRadius * 2,
                max: nodeRadius * 16,
              },
            },
          },
          edge: {
            width: {
              min: 1,
              max: nodeRadius * 2,
            }
          },
        },
      }, params);

      self.resetView();

      // shortcut to dimensions
      var dim = self.params.dimensions;

      // create arc generators
      self.arc = {
        progress: {
          map: d3.svg.arc()
            .innerRadius(0).outerRadius(dim.node.progress.map.radius)
            .startAngle(0).endAngle(function(a) { return a; }),
          reduce: d3.svg.arc()
            .innerRadius(0).outerRadius(dim.node.progress.reduce.radius)
            .startAngle(0).endAngle(function(a) { return a; })
        }
      };

      // create scale for node metric
      var magDomain = [10, 100, 1000, 10000];
      var magRadiusMin = dim.node.metric.radius.min;
      var magRadiusMax = dim.node.metric.radius.max;
      var magRadiusDelta = (magRadiusMax - magRadiusMin) / (magDomain.length + 1);
      var magRange = _.range(magRadiusMin, magRadiusMax, magRadiusDelta);
      self.metricScale = d3.scale.threshold().domain(magDomain).range(magRange);

      // TODO: Create node metric functions, menu

      // define edge metric functions
      self.edgeMetricFunctions = {
        none: {
          name: 'None',
          apply: function(sourceData, targetData) { return 0; },
        },
        mapInputRecords: {
          name: 'Map Input Records',
          heading: 'Target Node',
          apply: function(sourceData, targetData) {
            return JobData.getMapInputRecords(targetData);
          },
        },
        reduceOutputRecords: {
          name: 'Reduce Output Records',
          heading: 'Source Node',
          apply: function(sourceData, targetData) {
            return JobData.getReduceOutputRecords(sourceData);
          },
        },
        hdfsBytesRead: {
          name: 'HDFS Bytes Read',
          heading: 'Target Node',
          apply: function(sourceData, targetData) {
            return JobData.getHdfsBytesRead(targetData);
          },
        },
        hdfsBytesWritten: {
          name: 'HDFS Bytes Written',
          heading: 'Source Node',
          apply: function(sourceData, targetData) {
            return JobData.getHdfsBytesWritten(sourceData);
          },
        },
        fileBytesWritten: {
          name: 'File Bytes Written',
          heading: 'Source Node',
          apply: function(sourceData, targetData) {
            return JobData.getFileBytesWritten(sourceData);
          },
        },
      };

      // install edge metric dropdown menu
      self.installEdgeMetricMenu();

      // Ensure we resize appropriately
      $(window).resize(function(e) {
        // Prevent the DAG from flashing when the script div is resized.
        // TODO: Bind this callback more intelligently to avoid this hack
        if (!(e.target && e.target.classList && e.target.classList.contains('ambrose-view-script'))) {
          // Remove the popover before resize, otherwise there will be more than 1 popover.
          $('.popover').remove();
          self.resetView();
          self.handleJobsLoaded();
          self.rescaleEdges();
        }
      });

      // bind event workflow handlers
      workflow.on('jobsLoaded', function() {
        self.handleJobsLoaded();
      });
      workflow.on('jobStarted jobProgress jobComplete jobFailed', function(event, job) {
        self.handleJobUpdated(job);
      });
      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        self.handleMouseInteraction(_.reject([prev, job], _.isNull));
      });
    },

    getEdgeMetricFunction: function() {
      var f = this.edgeMetricFunction;
      if (f == null) {
        var name = localStorage.getItem('ambrose.view.graph.edgeMetricFunction.name');
        if (name != null) {
          f = _.find(this.edgeMetricFunctions, function (f) { return f.name == name; });
        }
        if (f == null) {
          f = this.edgeMetricFunctions.fileBytesWritten;
          this.setEdgeMetricFunction(f);
        }
      }
      return f;
    },

    setEdgeMetricFunction: function(f) {
      localStorage.setItem('ambrose.view.graph.edgeMetricFunction.name', f.name);
      this.edgeMetricFunction = f;
    },

    installEdgeMetricMenu: function() {
      var self = this;

      // create dropdown menu
      var nav = $('#main-navbar-collapse-right');
      var item = $('<li class="dropdown">').prependTo(nav);
      var toggle = $('<a id="edge-metric-button" href="#" class="dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" role="button" aria-expanded="false">')
          .appendTo(item)
          .html('Edge Metric <span class="caret"></span>');
      var menu = $('<ul id="edge-metric-menu" class="dropdown-menu" role="menu" aria-labelledby="workflow-button">')
          .appendTo(item);

      // group edge metric functions
      var edgeMetricFunctionsByHeading = _.groupBy(self.edgeMetricFunctions, function(edgeMetricFunction) {
        return (edgeMetricFunction.heading == null) ? '' : edgeMetricFunction.heading;
      });

      // create headings and buttons
      var buttons = _.chain(edgeMetricFunctionsByHeading)
          .pairs()
          .sortBy(function(pair) { return pair[0]; })
          .map(function(pair) {
            var heading = pair[0], edgeMetricFunctions = pair[1];

            // create heading
            if (heading.length > 0) {
              $('<li role="presentation" class="divider">').appendTo(menu);
              $('<li role="presentation" class="dropdown-header">').appendTo(menu).text(heading);
            }

            // create menu items
            return _.chain(edgeMetricFunctions)
              .sortBy(function(edgeMetricFunction) { return edgeMetricFunction.name; })
              .map(function(edgeMetricFunction) {
                var $button = edgeMetricFunction.button = $('<a href="#" role="menuitem" tabindex="-1">')
                    .appendTo($('<li role="presentation">').appendTo(menu))
                    .data('edge-metric-function', edgeMetricFunction)
                    .text(edgeMetricFunction.name);

                // get dom element from jquery selection
                return $button.get(0);
              })
              .value();
          })
          .flatten()
          .value();

      // add click handlers to buttons
      var $buttons = $(buttons);
      $buttons.click(function() {
        var $button = $(this);
        $buttons.removeClass('active');
        $button.addClass('active');
        self.setEdgeMetricFunction($button.data('edge-metric-function'));
        self.rescaleEdges();
      });

      // set current edge metric function button active
      self.getEdgeMetricFunction().button.addClass('active');
    },

    resetView: function() {
      var self = this;

      // initialize dimensions
      var container = self.container;
      var dim = self.dimensions = {};
      var width = dim.width = container.width();
      var height = dim.height = container.height();

      // define projection
      var xs = self.xs = d3.interpolate(0, width);
      var ys = self.ys = d3.interpolate(0, height);
      self.projection = function(d) { return [xs(d.x), ys(d.y)]; };

      // create canvas and primary groups
      self.svg = d3.select(container.empty().get(0))
        .append('svg:svg')
        .attr('class', 'ambrose-view-graph')
        .attr('width', width)
        .attr('height', height);
    },

    handleJobsLoaded: function() {
      var self = this;

      // compute node x,y coords
      var graph = self.workflow.graph;
      var groups = graph.topologicalGroups;
      var groupCount = groups.length;
      var groupDelta = 1 / groupCount;
      var groupOffset = groupDelta / 2;

      _.each(groups, function(group, i) {
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
        var pseudoIntervalDelta = 1.0 / (
          (realIntervals * pseudoToRealIntervalRatio) + pseudoIntervals
        );
        var realIntervalDelta = pseudoIntervalDelta * pseudoToRealIntervalRatio;
        var offset = realIntervalDelta / 2.0;

        // assign vertical offsets to nodes; create edges
        _.each(group, function(node, j) {
          node.x = x;
          node.y = offset;
          // TODO(Andy Schlaikjer): Fix to support mixed ordering of real / pseudo nodes
          offset += isReal(node) ? realIntervalDelta : pseudoIntervalDelta;
          var edges = node.edges = [];
          _.each(node.parents || [], function(parent) {
            edges.push({ source: parent, target: node });
          });
        });
      });

      // create svg elements
      var graph = self.workflow.graph;
      var nodes = graph.nodes.concat(graph.pseudoNodes).sort(function(a, b) {
        // sort nodes by topological group, desc
        return b.topologicalGroupIndex - a.topologicalGroupIndex;
      });
      var g = self.selectNodeGroups(nodes);
      self.removeNodeGroups(g);
      self.createNodeGroups(g);
      self.updateNodeGroups(g);

      // trigger event
      // TODO: Trigger on self, not workflow
      self.workflow.trigger('graph.view.initialized', [self.workflow.jobs]);
    },

    /**
     * Updates the node associated with the job, then rescales all edges.
     */
    handleJobUpdated: function(job) {
      var nodes = [ job.node ];
      this.updateNodeGroups(this.selectNodeGroups(nodes));
      this.rescaleEdges();
    },

    handleMouseInteraction: function(jobs) {
      var nodes = jobs.map(function(j) { return j.node; });
      this.updateNodeGroupsFill(this.selectNodeGroups(nodes));
    },

    selectNodeGroups: function(nodes) {
      return this.svg.selectAll('g').data(nodes, function(node) { return node.id; });
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
      var colors = self.params.colors;
      var edgeWidthMin = self.params.dimensions.edge.width.min;
      var edgeWidthMax = self.params.dimensions.edge.width.max;

      // create node group elements
      g = g.enter().append('svg:g').attr('class', function(node) {
        return node.pseudo ? 'pseudo' : 'node';
      });

      // create edges; each node references in-bound edges
      g.each(function(node, i) {
        function calcEdgeControlPoints(edge, i) {
          var p0 = edge.source,
              p3 = edge.target,
              m = (p0.x + p3.x) / 2,
              p = [p0, {x: m, y: p0.y}, {x: m, y: p3.y}, p3],
              p = p.map(projection);
          return 'M' + p[0] + 'C' + p[1] + ' ' + p[2] + ' ' + p[3];
        };

        // visible edge
        d3.select(this).selectAll('path.edge').data(node.edges).enter()
          .append('svg:path').attr('class', 'edge')
          .attr('stroke-width', edgeWidthMin)
          .attr('d', function(edge, i) {
            return calcEdgeControlPoints(edge, i);
          });

        // invisible edge for mouse interaction
        d3.select(this).selectAll('path.trigger').data(node.edges).enter()
          .append('svg:path').attr('class', 'trigger')
          .attr('stroke-width', edgeWidthMax)
          .attr('stroke', 'transparent')
          .attr('d', function(edge, i) {
            return calcEdgeControlPoints(edge, i);
          });
      });

      // filter down to real nodes
      var real = g.filter(isReal);

      // create translucent circle depicting node metric
      real.append('svg:circle')
        .attr('class', 'metric')
        .attr('cx', cx)
        .attr('cy', cy);

      // create arcs depicting MR task completion
      var progress = real.append('svg:g')
        .attr('class', 'progress')
        .attr('transform', function(d) {
          return 'translate(' + xs(d.x) + ',' + ys(d.y) + ')';
        });
      progress.append('svg:path')
        .attr('class', 'progress reduce');
      progress.append('svg:path')
        .attr('class', 'progress map');

      // create smaller circle and bind event handlers
      real.append('svg:circle')
        .attr('class', 'trigger')
        .attr('data-node-id', function(node, i) {
          return node.id;
        })
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
      var colors = self.params.colors;

      function getArcTween(progress, arc) {
        return function(d) {
          var b = progress.angle(d);
          var e = progress.angle(d, progressToAngle(progress.value(d)));
          var i = d3.interpolate(b, e);
          return function(t) {
            return arc(i(t));
          };
        };
      }

      // initiate transition
      t = g.transition().duration(duration);

      // udpate node fills
      self.updateNodeGroupsFill(t);

      // update map, reduce progress arcs
      t.selectAll('g.node path.map').attrTween('d', getArcTween(progress.map, self.arc.progress.map));
      t.selectAll('g.node path.reduce').attrTween('d', getArcTween(progress.reduce, self.arc.progress.reduce));

      // update metric radius
      t.selectAll('g.node circle.metric').attr('r', function(node) {
        var radius = 0;
        if (node && node.data && node.data.mapReduceJobState) {
          // compute current radius
          var jobState = node.data.mapReduceJobState;
          var totalTasks = jobState.totalMappers + jobState.totalReducers;
          radius = self.metricScale(totalTasks);

          // fetch previous radius
          var circle = $(this);
          var radiusPrev = circle.attr('r') || 0;
          var radiusDelta = radius - radiusPrev;
          if (radiusDelta != 0) {
            // radius changed; remove all tics immediately
            var $group = circle.parent();
            $group.find('circle.tic').remove();
            var group = d3.select($group[0]);

            // add all tics
            var cx = circle.attr('cx');
            var cy = circle.attr('cy');
            var factor = totalTasks;
            while (factor >= 10) {
              var radiusTic = self.metricScale(factor);
              var tic = group.insert('svg:circle', 'g.progress')
                  .attr('class', 'tic')
                  .attr('cx', cx).attr('cy', cy)
                  .attr('r', radiusTic);
              if (radiusTic > radiusPrev) {
                // new tic; animate opacity
                tic.attr('opacity', 0)
                  .transition().delay(duration).delay(500)
                  .attr('opacity', 1);
              }
              factor /= 10;
            }
          }
        }
        return radius;
      });
    },

    updateNodeGroupsFill: function(g) {
      var self = this;
      var colors = self.params.colors;

      function fill(node) {
        var job = node.data;
        var status = job.status || '';

        if (job.mouseover) { return colors.mouseover; }
        if (job.selected) { return colors.selected; }

        return colors[status.toLowerCase()] || colors.pending;
      }

      g.selectAll('g.node circle.trigger').attr('fill', fill);
    },

    /**
     * Computes edge metric min, max, then updates all edge widths.
     */
    rescaleEdges: function() {
      var self = this;
      var graph = this.workflow.graph;
      var edgeMetricFunction = self.getEdgeMetricFunction();

      // compute edge metric min and max
      var edgeMetricMin = Number.MAX_VALUE;
      var edgeMetricMax = Number.MIN_VALUE;
      _.each(graph.nodes, function(source) {
        _.each(source.children, function(target) {
          var metric = edgeMetricFunction.apply(source.data, target.data);
          if (metric == null) return;
          if (edgeMetricMin > metric) edgeMetricMin = metric;
          if (edgeMetricMax < metric) edgeMetricMax = metric;
        });
      });

      // define interpolation from edge metric to width
      var edgeWidthMin = self.params.dimensions.edge.width.min;
      var edgeWidthMax = self.params.dimensions.edge.width.max;
      var edgeMetricDelta = edgeMetricMax - edgeMetricMin;
      var getWidth = null;
      if (edgeMetricDelta == 0) {
        getWidth = function(metric) { return edgeWidthMin; };
      } else {
        var edgeWidthDelta = edgeWidthMax - edgeWidthMin;
        var edgeWidthMetricQuotient = edgeWidthDelta / edgeMetricDelta;
        getWidth = function(metric) {
          if (metric == null || metric == 0) return edgeWidthMin;
          return edgeWidthMin + edgeWidthMetricQuotient * (metric - edgeMetricMin);
        }
      }

      // update stroke width of edges
      var duration = 500;
      var nodes = graph.nodes.concat(graph.pseudoNodes);
      var g = self.selectNodeGroups(nodes);
      g.each(function(node, i) {
        d3.select(this).selectAll('path.edge').data(node.edges)
          .transition().duration(duration)
          .attr('stroke-width', function(d, i) {
            var sourceData = d.source.pseudo ? d.source.source.data : d.source.data;
            var targetData = d.target.pseudo ? d.target.target.data : d.target.data;
            return getWidth(edgeMetricFunction.apply(sourceData, targetData)) + 'px';
          });
      });
    },
  };

  // bind prototype to ctor
  Graph.fn.init.prototype = Graph.fn;
  return Graph;
});
