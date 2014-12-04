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
 * This module defines the Graph class for representation and manipulation of directed acyclic
 * graphs. Graph nodes may be created from arbitrary javascript objects, provided a suitable 'getId'
 * and 'getParentIds' functions are defined.
 */
define(['lib/jquery', './core'], function($, Ambrose) {
  /**
   * Creates graph nodes from data and initializes indices for node access.
   */
  var _initNodes = function() {
    // build nodes and node indices
    var self = this;
    var nodes = self.nodes = [];
    var nodesById = self.nodesById = {};
    $.each(self.data, function(i, d) {
      self.addNode({
        id: self.getId(d),
        data: d,
      });
    });

    // initialize edges
    $.each(nodes, function(i, node) {
      var parentIds = self.getParentIds(node.data);
      if (!parentIds) return;
      $.each(parentIds, function(parentIndex, parentId) {
        var parent = nodesById[parentId];
        if (!parent) {
          console.warn('No node with id "' + parentId + '" exists:', self);
          return;
        }
        self.addEdge(node, parent);
      });
    });
  };

  /**
   * Creates pseudo nodes along an edge from child to parent, one for each
   * topological group the edge crosses.
   */
  var _createPseudoNodes = function(child, parent) {
    var cg = child.topologicalGroupIndex;
    var pg = parent.topologicalGroupIndex;
    if (cg - pg < 2) return;

    // init refs
    var graph = this;
    var nodes = graph.nodes;
    var nodesById = graph.nodesById;
    var groups = graph.topologicalGroups;

    // remove child to parent edge
    graph.removeEdge(child, parent);

    // for each intermediate topological group
    var prev = child;
    for (var g = cg - 1; g > pg; g--) {
      // create pseudo node
      var node = graph.addNode({
        pseudo: true,
        id: child.id + ':' + parent.id + ':' + g,
        topologicalIndex: -1, // not generally useful
        topologicalGroupIndex: g,
        targetData: parent.data,
        sourceData: child.data
      });

      // add pseudo node to group
      groups[g].push(node);

      // add prev to node edge
      graph.addEdge(prev, node);
      prev = node;
    }

    // add prev to parent edge
    graph.addEdge(prev, parent);
  };

  /**
   * Primary ctor.
   */
  var Graph = Ambrose.Graph = function(params) {
    return new Ambrose.Graph.fn.init(params);
  };

  /**
   * Graph prototype.
   */
  Graph.fn = Graph.prototype = {
    /**
     * Constructor.
     *
     * @param params a map of parameter values. Valid parameters include: "data"
     * an array of arbitrary objects; "getId" (optional) a function used to
     * extract a unique identifier from each data element; "getParentIds"
     * (optional) a function used to extract an array of ids of the parents of
     * each data element.
     */
    init: function(params) {
      this.data = params.data;
      this.getId = params.getId || function(d) { return d.id; };
      this.getParentIds = params.getParentIds || function(d) { return d.parentIds; };
      _initNodes.call(this);
    },

    /**
     * Adds a node to the graph.
     */
    addNode: function(params) {
      var node = $.extend(params, {
        parents: [],
        parentsById: {},
        children: [],
        childrenById: {},
      });
      this.nodesById[node.id] = node;
      var nodes = node.pseudo ? this.pseudoNodes : this.nodes;
      node.index = nodes.push(node) - 1;
      return node;
    },

    /**
     * Adds a directed edge to the graph.
     */
    addEdge: function(from, to) {
      from.parents.push(to);
      from.parentsById[to.id] = to;
      to.children.push(from);
      to.childrenById[from.id] = from;
    },

    /**
     * Removes a directed edge from the graph.
     */
    removeEdge: function(from, to) {
      from.parents.remove(to);
      delete from.parentsById[to.id];
      to.children.remove(from);
      delete to.childrenById[from.id];
    },

    /**
     * Sorts nodes topologically, assigning each node properties 'topologicalIndex' and
     * 'topologicalGroupIndex'. The graph itself is assigned a 'topologicalGroups' property which
     * references an array of arrays of nodes.
     */
    sort: function() {
      // state
      var g = {}; // partial nodes by id for computation
      var q = []; // nodes with zero out degree
      var nodes = this.nodes;
      var nodesById = this.nodesById;
      var nodesByTopologicalIndex = this.nodesByTopologicalIndex = [];
      var topologicalGroups = this.topologicalGroups = [];

      // create working copy of graph, find roots
      $.each(nodes, function(i, node) {
        var n = g[node.id] = {
          id: node.id,
          outDegree: node.parents ? node.parents.length : 0,
          children: $.map(node.children || [], function(n, i) { return n.id; }),
        };
        if (n.outDegree == 0) q.push(n);
      });

      // assign topological and group indices to nodes
      while (q.length > 0) {
        var q2 = [];
        var topologicalGroup = [];
        var topologicalGroupIndex = topologicalGroups.push(topologicalGroup) - 1;

        $.each(q, function(i, n) {
          $.each(n.children, function(j, id) {
            var c = g[id];
            if (!c) return;
            if (--c.outDegree == 0) q2.push(c);
          });

          var node = nodesById[n.id];
          node.topologicalIndex = nodesByTopologicalIndex.push(node) - 1;
          node.topologicalGroupIndex = topologicalGroupIndex;
          topologicalGroup.push(node);
        });

        q = q2;
      }

      // push nodes down to lowest possible topological group
      $.each(nodes, function(i, node) {
        var children = node.children;
        if (!children || children.length == 0) return;
        var groupIndices = $.map(children, function(c) { return c.topologicalGroupIndex; });
        var minGroupIndex = groupIndices.min() - 1;
        if (node.topologicalGroupIndex >= minGroupIndex) return;
        var oldGroup = topologicalGroups[node.topologicalGroupIndex];
        var newGroup = topologicalGroups[minGroupIndex];
        oldGroup.remove(node);
        newGroup.push(node);
        node.topologicalGroupIndex = minGroupIndex;
      });
    },

    /**
     * Once a graph is topologically sorted via sort(), additional nodes may be
     * added to the graph to simplify routing of edges which span multiple
     * topological groups. For instance, given child node C in group 2 and
     * parent node P in group 0, a new node CP would be added to group 1 and the
     * edge from C to P would be split such that C links to CP and CP to P.
     */
    addPseudoNodes: function() {
      this.pseudoNodes = [];
      var graph = this;
      var groups = this.topologicalGroups;
      for (var g = 2; g < groups.length; g++) {
        var group = groups[g];
        $.each(group, function(i, node) {
          $.each(node.parents.concat(), function(j, parent) {
            _createPseudoNodes.call(graph, node, parent);
          });
        });
      }
    },

    /**
     * Once a graph is topologically sorted via sort(), the nodes in each
     * topological group may be reordered to attempt to minimize edge crossings
     * between groups. This problem is in general NP-complete, so heuristics are
     * used here to achieve a reasonable approximation in time linear to number
     * of nodes.
     */
    sortTopologicalGroups: function() {
      // TODO
    },

    /**
     * Creates a dense square transition matrix encoding number of directed edges between nodes,
     * suitable for use with d3's chord layout.
     */
    buildTransitionMatrix: function(topological) {
      if (topological == null && this.nodesByTopologicalIndex != null) topological = true;
      var getIndex = topological ?
        function(n) { return n.topologicalIndex; }
      : function(n) { return n.index; };
      var nodes = this.nodes;
      var matrix = [];
      for (var i = 0; i < nodes.length; i++) {
        var row = matrix[i] = [];
        for (var j = 0; j < nodes.length; j++) {
          row[j] = 0;
        }
      }
      $.each(nodes, function(i, node) {
        var c = getIndex(node);
        $.each(node.parents, function(i, parent) {
          while (parent.pseudo) parent = parent.parents[0];
          var p = getIndex(parent);
          if (p == null) {
            console.log('p is null');
          }
          matrix[c][p]++;
        });
      });
      return matrix;
    },
  };

  // bind prototype to ctor
  Graph.fn.init.prototype = Graph.fn;
  return Graph;
});
