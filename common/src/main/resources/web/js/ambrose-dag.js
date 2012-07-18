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
 * Ambrose module "dag" controls the directed acyclic graph view of the
 * workflow.
 */
(function($, ambrose) {
  var dag = ambrose.dag = function(ui) {
    return new ambrose.dag.fn.init(ui);
  };

  dag.fn = dag.prototype = $.extend(ambrose.chart(), {
    /**
     * Constructor initializes public fields and binds to ui events.
     */
    init: function(ui) {
      ambrose.chart.fn.init.call(this, ui, "dagView", "Dag");
      this.minX = Infinity;
      this.maxX = -Infinity;
      this.minY = Infinity;
      this.maxY = -Infinity;
    },

    initChart: function(jobs) {
      var nodeWidth = 20;
      var canvasWidth = $('#vizGroup').width();
      var canvasHeight = 380;
      var halfCanvasWidth = canvasWidth / 2;
      var halfCanvasHeight = canvasHeight / 2;
      var padding = 40;
      var viewWidth = canvasWidth - (2 * padding);
      var viewHeight = canvasHeight - (2 * padding);

      var minX = this.minX;
      var maxX = this.maxX;
      var minY = this.minY;
      var maxY = this.maxY;

      var json = jobs.map(function(n) {
        var x = n.x, y = n.y;

        // get bounding box
        maxX = maxX < x ? x : maxX;
        minX = minX > x ? x : minX;

        maxY = maxY < y ? y : maxY;
        minY = minY > y ? y : minY;

        return {
          id: n.name,
          name: n.index + 1,
          data: n,
          adjacencies: n.successorNames
        };
      });

      var diffX = maxX - minX;
      var diffY = maxY - minY;

      // create and configure the dag view
      var viz = this.viz = new $jit.ST({
        injectInto: 'dagView',
        width: canvasWidth,
        height: canvasHeight,

        Navigation: {
          enable: true,
          panning: 'avoid nodes',
          zooming: 10 // zoom speed. higher is more sensible
        },

        Node: {
          //overridable: true,
          type: 'none',
          width: nodeWidth,
          height: 20,
          align: 'left'
        },

        Edge: {
          overridable: true,
          type: 'line',
          color: '#23A4FF',
          lineWidth: 0.8
        },

        // Native canvas text styling
        Label: {
          type: 'HTML',
          size: 12,
          style: 'bold'
        },

        Events: {
          enable: true,

          // Update node positions when dragged
          onDragMove: function(node, eventInfo, e) {
            viz.tooltipPinned = true;
            viz.tips.tip.style.display = 'none';

            var pos = eventInfo.getPos();
            node.pos.setc(pos.x, pos.y);
            viz.plot();
          },

          onDragEnd: function() {
            viz.tooltipPinned = false;
          },

          onDragCancel: function() {
            viz.tooltipPinned = false;
          }
        },

        // Enable tips
        Tips: {
          enable: true,

          // add positioning offsets
          offsetX: 20,
          offsetY: 20,

          /*
           * implement the onShow method to add content to the tooltip when a
           * node is hovered over.
           */
          onShow: function(tip, node, isLeaf, domElement) {
              var whiteList = [{ key: 'jobId', name: 'Job ID'},
                               { key: 'aliases', name: 'Aliases'},
                               { key: 'features', name: 'Features'},
                               { key: 'map progress', name: 'Mappers'},
                               { key: 'reduce progress', name: 'Reducers'}],
                  data = node.data,
                  html = "<div class=\"tip-title\">" + node.name
                    + "</div><div class=\'closetip\'>&#10006;</div><div class=\"tip-text\"><ul>";

            whiteList.forEach(function(k) {
              if (k.key in data) {
                html += "<li><b>" + k.name + "</b>: ";
                if (k.key == 'jobId' && data['trackingUrl'] != null) {
                  html += "<a href=\"" + data['trackingUrl'] + "\" target=\"__blank\">" + data[k.key] + "</a>";
                } else {
                  html += '<span id="' + data.jobId + '_' + k.key.replace(/\s+/, '_') + '">' + data[k.key] + '</span>';
                }
                html += "</li>";
              }
            });

            tip.innerHTML =  html + "</ul></div>";
          }
        },

        /*
         * Add text to the labels. This method is only triggered on label
         * creation and only for DOM labels (not native canvas ones).
         */
        onCreateLabel: function(domElement, node){
          domElement.innerHTML = node.name;
          var style = domElement.style;
          style.fontSize = "0.8em";
          style.color = "black";
          style.width = nodeWidth + 'px';

          domElement.addEventListener('click', function(e) {
            var tips = viz.tips;
            if (tips.tip.classList.contains('pinned')) {
              viz.tooltipPinned = false;
              tips.tip.classList.remove('pinned');
              tips.onMouseOut.call(tips, e, window);
            } else {
              viz.tooltipPinned = !viz.tooltipPinned;
              tips.tip.classList.toggle('pinned');
            }
          }, false);
        },

        /*
         * Change node styles when DOM labels are placed or moved.
         */
        onPlaceLabel: function(domElement, node){
          var style = domElement.style;
          var left = parseInt(style.left);
          var top = parseInt(style.top);
          var w = domElement.offsetWidth;
          var h = node.getData('height');

          domElement.style.width = (nodeWidth * Math.max(1, viz.canvas.scaleOffsetX)) + 'px';

          style.left = left + 'px';
          style.top = top + 'px';

          var height = domElement.offsetHeight;

          style.top = (top + (height ? -height/2 + 10 : 0)) + 'px';

        }
      });

      // load JSON data
      viz.loadJSON(json);

      // reposition nodes
      viz.graph.eachNode(function(n) {
        n.pos.setc(diffX
                   ? ((n.data.x - minX) / diffX * viewWidth - halfCanvasWidth + padding)
                   : (n.data.x - halfViewWidth),
                   diffY
                   ? ((n.data.y - minY) / diffY * viewHeight - halfCanvasHeight + padding)
                   : n.data.y);

        n._depth = n.data.dagLevel;
        n.exist = true;
        n.drawn = true;

        n.eachAdjacency(function(adj) {
          var nodeFrom = adj.nodeFrom,
          nodeTo = adj.nodeTo;
          if (nodeFrom._depth < nodeTo._depth) {
            adj.setData('direction', [nodeFrom.id, nodeTo.id]);
          } else {
            adj.setData('direction', [nodeTo.id, nodeFrom.id]);
          }
        });
      });

      this.extendViz(viz);

      viz.plot();
    },

    extendViz: function(viz) {
      // Add pinned tooltip behavior
      var tips = viz.tips;
      var onMouseMove = tips.onMouseMove;
      var onMouseOut  = tips.onMouseOut;
      var onMouseOver = tips.onMouseOver;

      var wrapper = function (cond, fn) {
        return function() {
          if (cond.apply(this, arguments)) {
            return fn.apply(this, arguments);
          }
        };
      };

      var cond = function() {
        return !viz.tooltipPinned;
      };

      var hidetip = function(e) {
        viz.tooltipPinned = false;
        viz.tips.tip.classList.remove('pinned');
        tips.onMouseOut.call(tips, e, window);
      };

      tips.onMouseMove = wrapper(cond, onMouseMove);
      tips.onMouseOver = wrapper(cond, onMouseOver);
      tips.onMouseOut  = wrapper(cond, onMouseOut);

      tips.tip.addEventListener('click', function(e) {
        if (~e.target.className.indexOf('closetip')) {
          hidetip(e);
        }
      }, false);

      viz.canvas.canvases[0].canvas.addEventListener('mousedown', hidetip, false);
      viz.canvas.canvases[0].canvas.addEventListener('mousewheel', hidetip, false);
      viz.canvas.canvases[0].canvas.addEventListener('DOMMouseScroll', hidetip, false);

      document.querySelector('a[href="#chordView"]').addEventListener('click', hidetip, false);
    },

    refresh: function(event, data) {
      var viz = this.viz;
      if (!viz) return;

      var type = event.type;
      var id = data.job.name;
      var job = data.job;
      var $id = function(d) { return document.getElementById(d); };
      var entry;
      var n = viz.graph.getNode(id);

      n.data.status = type;

      if (job.mapProgress) {
        n.data['map progress'] = AMBROSE.util.task_progress_string(job.totalMappers, job.mapProgress);
        entry = $id(job.jobId + '_map progress');
        if (entry) {
          entry.innerHTML = n.data['map progress'];
        }
      }

      if (job.reduceProgress) {
        n.data['reduce progress'] = AMBROSE.util.task_progress_string(job.totalReducers, job.reduceProgress);
        entry = $id(job.jobId + '_reduce progress');
        if (entry) {
          entry.innerHTML = n.data['reduce progress'];
        }
      }

      if (job.trackingUrl) {
        n.data['trackingUrl'] = job.trackingUrl;
      }

      if (n) {
        var label = viz.fx.labels.getLabel(n.id),
        update = false;

        if (type == 'JOB_FINISHED') {
          label.className = 'node ' + type;
          n.eachAdjacency(function(a) {
            a.setData('color', '#aaa');
          });
          update = true;
        } else if (type == 'JOB_FAILED') {
          label.className = 'node ' + type;
          n.eachAdjacency(function(a) {
            a.setData('color', '#c00');
          });
          update = true;
        } else if (type == 'jobSelected') {
          label.className = 'node ' + type;
        }

        if (update) {
          viz.fx.plot();
        }
      }
    }
  });

  // set the init function's prototype for later instantiation
  dag.fn.init.prototype = ambrose.dag.fn;

}(jQuery, AMBROSE));
