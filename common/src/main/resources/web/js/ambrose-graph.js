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
 * Graph module which provides a simple graph class along with common methods
 * for access and manipulation nodes.
 */
define(['jquery', 'ambrose'], function($, ambrose) {
  var graph = ambrose.graph = function(params) {
    return new ambrose.graph.fn.init(params);
  };

  /**
   * Creates graph nodes from data and initializes indices for node access.
   */
  var _initNodes = function() {
    // build nodes and node indices
    var g = this;
    var nodes = g.nodes = [];
    var nodesById = g.nodesById = {};
    $.each(g.data, function(i, d) {
      g.addNode({
        id: g.getId(d),
        data: d,
      });
    });

    // initialize edges
    $.each(nodes, function(i, node) {
      var parentIds = g.getParentIds(node.data);
      if (!parentIds) return;
      $.each(parentIds, function(parentIndex, parentId) {
        var parent = nodesById[parentId];
        if (!parent) {
          console.warn('No node with id "' + parentId + '" exists');
          return;
        }
        g.addEdge(node, parent);
      });
    });
  };

  /**
   * Creates pseudo nodes along an edge from child to parent, one for each
   * topological group the edge crosses.
   */
  var _createPseudoNodes = function(child, parent) {
    var cgi = child.topologicalGroupIndex;
    var pgi = parent.topologicalGroupIndex;
    if (cgi - pgi < 2) return;

    // init refs
    var graph = this;
    var nodes = graph.nodes;
    var nodesById = graph.nodesById;
    var topologicalGroups = graph.topologicalGroups;

    // remove child to parent edge
    graph.removeEdge(child, parent);

    // for each intermediate topological group
    var prev = child;
    for (var gi = cgi - 1; gi > pgi; gi--) {
      var g = topologicalGroups[gi];

      // create pseudo node
      var node = g.addNode({
        pseudo: true,
        id: child.id + ':' + parent.id + ':' + gi,
        topologicalIndex: -1, // not generally useful
        topologicalGroupIndex: gi,
      });
      g.push(node);

      // add prev to node edge
      graph.addEdge(prev, node);Q
    }

    // add prev to parent edge
    graph.addEdge(prev, parent);
  };

  graph.fn = graph.prototype = {
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
      node.index = this.nodes.push(node) - 1;
      this.nodesById[node.id] = node;
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
     * Sorts nodes topologically, assigning each node a topologicalIndex and
     * topologicalGroupIndex property. The graph itself also receives two new
     * properties: nodesByTopologicalIndex and topologicalGroups.
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
      var graph = this;
      for (var g = 2; g < graph.topologicalGroups.length; g++) {
        var group = graph.topologicalGroups[g];
        $.each(group, function(i, node) {
          $.each(node.parents, function(j, parent) {
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
  };

  graph.fn.init.prototype = graph.fn;

  return graph;
});
