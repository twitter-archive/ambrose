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
 * Ambrose module "chord" provides a chord diagram of the job graph.
 */
define(['lib/jquery', 'lib/d3', '../core', './core'], function(
  $, d3, Ambrose, View
) {
  // Chord ctor
  var Chord = View.Chord = function(workflow, container, params) {
    return new View.Chord.fn.init(workflow, container, params);
  }

  /**
   * Chord prototype.
   */
  Chord.fn = Chord.prototype = {
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
        dimensions: {
          padding: 20,
          radius: {
            min: 40,
            max: 200,
          },
          groupSpacing: 0.1,
          groupThickness: 16,
          labelMargin: 10,
          labelSize: 12,
        },
      }, View.Theme, params);

      // initialize dimensions
      var dim = this.dimensions = {};
      var padding = params.dimensions.padding;
      var maxRadius = params.dimensions.radius.max;
      var minRadius = params.dimensions.radius.min;
      var groupThickness = params.dimensions.groupThickness;
      var labelMargin = params.dimensions.labelMargin;
      var labelSize = params.dimensions.labelSize;
      var paddingTotal = labelMargin + labelSize + padding;
      var width = dim.width = container.width();
      var height = dim.height = container.height();
      var minDim = Math.min(width, height);
      var radius = dim.radius = Math.max(minRadius, Math.min(maxRadius, minDim / 2 - paddingTotal));
      var innerRadius = dim.innerRadius = Math.max(minRadius, radius) - groupThickness;

      // create canvas
      var svg = this.svg = d3.select(container.empty().get(0))
        .append('svg:svg')
        .attr('class', 'ambrose-view-chord')
        .attr('width', width)
        .attr('height', height)
        .append('svg:g')
        .attr('transform', 'translate(' + (width/2) + ',' + (height/2) + ')')
        .append('svg:g')
        .attr('transform', 'rotate(0)');

      // create arc generators
      this.arc = {
        job: d3.svg.arc().innerRadius(innerRadius).outerRadius(radius),
        mouse: d3.svg.arc().innerRadius(minRadius).outerRadius(radius)
          .endAngle(function(group) {
            return group.startAngle + self.dimensions.groupAngle;
          }),
      };

      // create closures to pass to d3
      this.f = {
        chordFill: function(d) { return self.jobColor(d.source.group.job); },
        chordStroke: function(d) { return d3.rgb(self.f.chordFill(d)).darker(); },
        chordOpacity: function(d) {
          return !(self.workflow.current.mouseover) ? 0.8
            : (d.source.group.job.mouseover ? 0.8 : 0.2);
        },
        groupFill: function(d) { return self.jobColor(d.job); },
        groupMouseOver: function(d, i) { self.handleGroupMouseOver(d, i); },
        groupMouseOut: function(d, i) { self.handleGroupMouseOut(d, i); },
        groupClick: function(d, i) { self.handleGroupClick(d, i); },
      };

      // bind event workflow handlers
      workflow.on('jobsLoaded', function(event, jobs) {
        self.handleJobsLoaded(jobs);
      });
      workflow.on('jobStarted jobProgress jobComplete jobFailed', function(event, job) {
        self.handleJobUpdated(350);
      });
      workflow.on('jobSelected jobMouseOver', function(event, job, prev) {
        self.handleJobUpdated();
      });
    },

    handleJobsLoaded: function(jobs) {
      var self = this;
      var svg = this.svg;

      // initialize dimensions dependent on job count
      var jobCount = jobs.length;
      var dim = this.dimensions;
      var params = this.params;
      var ga = dim.groupAngle = 2 * Math.PI / jobCount;
      var gas = dim.groupAngleSpacing = ga * params.dimensions.groupSpacing;
      var ga2 = dim.groupAngleHalf = (ga - gas) / 2;
      var radius = dim.radius;
      var innerRadius = dim.innerRadius;
      var labelMargin = params.dimensions.labelMargin;
      var labelSize = params.dimensions.labelSize;

      // initialize fills from palettes
      var fillCount = Math.min(7, Math.max(3, jobCount));
      this.fills = {
        queued: d3.scale.ordinal().range(this.params.palettes.queued[fillCount]),
        complete: d3.scale.ordinal().range(this.params.palettes.complete[fillCount]),
        failed: d3.scale.ordinal().range(this.params.palettes.failed[fillCount]),
      };

      // construct transition matrix
      var matrix = this.workflow.matrix
        || (this.workflow.matrix = this.workflow.graph.buildTransitionMatrix());

      // initialize chord layout
      var diagram = this.diagram = d3.layout.chord();
      diagram.matrix(matrix);
      var groups = this.groups = diagram.groups();
      var chords = this.chords = diagram.chords();

      // redefine group start/end angles
      $.each(groups, function(i, group) {
        var startAngle = ga * i;
        var endAngle = ga * (i + 1) - gas;
        var angle = startAngle + ga2;
        $.extend(group, {
          job: jobs[i],
          startAngle: startAngle,
          endAngle: endAngle,
          angle: angle,
          sources: [],
          targets: [],
        });
      });

      // collect chord source/target objects
      $.each(chords, function(i, chord) {
        var s = chord.source;
        var t = chord.target;
        var sg = s.group = groups[s.index];
        var tg = t.group = groups[t.index];
        sg.sources.push(s);
        tg.targets.push(t);
        chord.id = sg.job.name + '-' + tg.job.name;
        /*
         * The following sort values allow proper ordering of group sources and targets when the
         * underlying graph is not topologically sorted. With topological sorting, we're guaranteed
         * that a group's sources' subindices reference earlier groups. Similarly, a group's
         * targets' indices reference later groups.
         */
        //s.sortValue = Math.sin(groups[s.subindex].startAngle - sg.startAngle);
        //t.sortValue = Math.sin(groups[t.subindex].startAngle - tg.startAngle);
      });

      // sort group source/target objects and redefine start/end angles
      var sourceComparator = function(a, b) { return b.subindex - a.subindex; };
      var targetComparator = function(a, b) { return b.subindex - a.subindex; };
      $.each(groups, function(i, group) {
        group.sources.sort(sourceComparator);
        group.targets.sort(targetComparator);
        var a = group.startAngle;
        var ad = ga2 / group.sources.length;
        $.each(group.sources, function(i, d) {
          d.sortIndex = i;
          d.startAngle = a;
          d.endAngle = (a += ad);
        });
        a = group.angle;
        ad = ga2 / group.targets.length;
        $.each(group.targets, function(i, d) {
          d.sortIndex = i;
          d.startAngle = a;
          d.endAngle = (a += ad);
        });
      });

      // update chords
      var vchord = svg.selectAll('path.chord').data(chords, function(chord) {
        return chord.id;
      });
      // remove chords
      vchord.exit().remove();
      // create chords
      vchord.enter().append('svg:path').attr('class', 'chord')
        .style('fill', this.f.chordFill)
        .style('opacity', this.f.chordOpacity)
        .attr('d', d3.svg.chord().radius(innerRadius + 2));

      // update groups
      var vgroup = svg.selectAll('g.group').data(groups, function(group) { return group.job.name; });
      // remove groups
      vgroup.exit().remove();
      // create groups
      var vgroupEnter = vgroup.enter().append('svg:g').attr('class', 'group');
      // add arc
      vgroupEnter.append('svg:path')
        .attr('class', 'arc')
        .attr('d', this.arc.job)
        .style('fill', this.f.groupFill);
      // add label
      vgroupEnter.append('svg:text')
        .attr('font-size', labelSize)
        .attr('dy', labelSize / 2)
        .attr('text-anchor', function(d) {
          return d.angle > Math.PI ? 'end' : null;
        })
        .attr('transform', function(d) {
          return 'rotate(' + (d.angle * 180 / Math.PI - 90) + ')'
            + 'translate(' + (radius + labelMargin) + ')'
            + (d.angle > Math.PI ? 'rotate(180)' : '');
        })
        .text(function(d) { return d.index + 1; });
      // add arc to simplify mouse interaction
      vgroupEnter.append('svg:path')
        .attr('class', 'arc-mouse')
        .attr('d', this.arc.mouse)
        .style('fill', 'white')
        .style('opacity', '0')
        .on('mouseover', this.f.groupMouseOver)
        .on('mouseout', this.f.groupMouseOut)
        .on('click', this.f.groupClick);
    },

    handleJobUpdated: function(duration) {
      var svg = this.svg;
      var f = this.f;
      var chord = svg.selectAll('path.chord');
      var arc = svg.selectAll('path.arc');
      if (duration) {
        chord = chord.transition().duration(duration);
        arc = arc.transition().duration(duration);
      }
      chord.style('fill', f.chordFill).style('opacity', f.chordOpacity);
      arc.style('fill', f.groupFill);
    },

    handleGroupMouseOver: function(group) {
      this.workflow.mouseOverJob(group.job);
    },

    handleGroupMouseOut: function(group) {
      this.workflow.mouseOverJob(null);
    },

    handleGroupClick: function(group) {
      this.workflow.selectJob(group.job);
    },

    jobColor: function(job) {
      if (job.mouseover) return this.params.colors.mouseover;
      if (job.selected) return this.params.colors.selected;
      var status = (job.status || '').toLowerCase();
      var color = this.params.colors[status];
      if (color != null) return color;
      // TODO(Andy Schlaikjer): remove darker transform
      var fill = this.fills[status] || this.fills.queued;
      return d3.hsl(fill(job.index)).darker(0.5);
    },
  };

  // bind prototype to ctor
  Chord.fn.init.prototype = Chord.fn;
  return Chord;
});
