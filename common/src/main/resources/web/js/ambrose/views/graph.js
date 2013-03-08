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
      
 			// creating tip on node click
      var vis = d3.select("#ambrose-views-graph")		
		.append("svg:svg")
		.attr("id","job-sub-graph")
		.style("position", "absolute")

 	vis.append("svg:rect")					
		.attr("id","tip")
		.on('click', function(node) {
		  	 d3.select("#job-sub-graph-view").remove();
 		  	 d3.select("#job-sub-graph").attr("visibility","hidden");
		    });
				    
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
        .on('mouseout', function(node) { self.workflow.mouseOverJob(null)
        })
        .on('click',function(node){  
      		//sending request to get physical plan
        	data = node.data;
        	url = data['name'];
        	x  = (event.pageY);
        	y = (event.pageX);
        	
        	d3.select("#job-sub-graph-view").remove();
        	d3.json(url + '-subg',function(subg) {
						drawingPhysicalPlan(x,y,subg);
							} );	
		      }
        );
        
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

/** Draw tree with given values*/
function drawTree(treeData, treeWidth, treeHeight, vis ,linkColor, nodeColor ,delta){    
	var tree = d3.layout.tree()
		.size([treeWidth,treeHeight]);

	// preparing nodes 
	var nodes = tree.nodes(treeData);
	// Preparing links 
	var links = tree.links(nodes);
			

	// Drawing mapper
	 var diagonal = d3.svg.diagonal()
			.projection(function(d) { return [treeHeight - d.y + delta , d.x]; });
		    

	var link = vis.selectAll("pathlink")
			.data(links)
			.enter().append("svg:path")
			.attr("class", "link")
			.attr("d", diagonal)
			.style("stroke-width", "5px")
			.style("stroke", linkColor)
			.style("fill", "none")
			.attr("dx",+15)
			;

	var node = vis.selectAll("g.node")
			.data(nodes)
			.enter().append("svg:g")
			.attr("transform", function(d) { return "translate(" + ((treeHeight- d.y) + delta) + "," + d.x  + ")"; })
			;

	// Add the dot at every node
	node.append("svg:circle")
			.attr("r", 25)
			.style("stroke",nodeColor)
			.style("fill","#FFFFFF");
			
	//adding title
	node.append("title")
			.text(function(d) { return d.name; });

	//placing a substring of name as text
	node.append("text")
			.style("text-anchor", "middle")
			.text(function(d) { return d.name.substring(0, 25/3); })
			.style("font-size","10px");
}

/** Drawing mapper and reducer*/
function drawingPhysicalPlan(x,y,subg){

	var mapperData = subg[0].tree;
	var mapperLevel = subg[0].level;
	var reducerData = subg[1].tree;
	var reducerLevel = subg[1].level;

	//Getting boarder of tree	
	var w = 50;
	var h =50;
	var mapperWidth = mapperLevel*w;
	var mapperHeight = mapperLevel*h;
	var reducerWidth = reducerLevel*w;
	var reducerHeight = reducerLevel*h;

	var maxWidth = mapperWidth > reducerWidth?  mapperWidth :reducerWidth;
	var maxHeight = mapperHeight > reducerHeight?  mapperHeight :reducerHeight;

	//color for mapper and reducer
	var mapperLinkColor = "#63B8FF";
	var reducerLinkColor = "#8470FF";
	var mapperNodeColor = "#4682B4";
	var reducerNodeColor = "#483D8B";

	//remove old one
	d3.select("#job-sub-graph").select("#job-sub-graph-view").remove();
	d3.select("#job-sub-graph").attr("visibility","hidden");
	//resizing tip to fit mapper and reducer
	d3.select("#tip").style("z-index", "50")
			.attr("width", 2*maxHeight+200)
			.attr("height", maxWidth)
			.attr("fill","white")
			.attr("stroke","gray")
			.attr("rx",20)
			.attr("ry",20)
			.attr("stroke-width",5);
	
	//moving tip to node 
	d3.select("#job-sub-graph").style("z-index", "50")
			.style("top", x +"px").style("left", y +"px");
			
	//changing tip size according to physical plan size	
	 var vis= d3.select("#job-sub-graph")
	 		.append("svg:g")
			.attr("id","job-sub-graph-view")			
			.attr("dx", +100)
			.attr("width", 2*maxHeight+ 200)
			.attr("height", maxWidth)	  

				
	//drawing mapper
	drawTree( mapperData, maxWidth, maxHeight, vis ,mapperLinkColor,mapperNodeColor,50);
	//drawing reducer
	drawTree( reducerData, maxWidth, maxHeight, vis ,reducerLinkColor,reducerNodeColor,maxHeight+ 150);

	//drawing line between mapper and reducer
	vis.append("line")
			.attr("x1", (maxHeight+100))
			.attr("y1", 0)
			.attr("x2", (maxHeight+100))
			.attr("y2", maxWidth)
			.style("fill","none")
			.style("stroke","#ccc");
		
	//adding text for mapper and reducer
	vis.append("svg:text")
			.attr("dy", +20)
			.attr("dx", +15)
			.style("font-size","15px")
			.text("Job Mapper")
			.style("fill",mapperNodeColor);

	vis.append("svg:text")
			.attr("dy", +20)
			.attr("dx", (maxHeight+105))
			.style("font-size","15px")
			.text("Job Reducer")
			.style("fill",reducerNodeColor);

	d3.select("#job-sub-graph").attr("visibility","visible");
}
