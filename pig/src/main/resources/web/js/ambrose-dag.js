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
AMBROSE.dagView = function () {

  var viz;

  return {
    divName: "dagView",
    tabName: "Dag",

    minX: Infinity,
    maxX: -Infinity,
    minY: Infinity,
    maxY: -Infinity,

    addDiv: function() {
      // add the div that the graph will render in
      $('#vizGroup').append('<div class="viz-pane tab-pane" id="' + this.divName + '"></div>');

      // add the tab div
      $('#vizTabs').append('<li><a href="#' + this.divName + '" data-toggle="tab">' + this.tabName + '</a></li>');
    },

    initGraph: function(jobs) {
        this.addDiv();

        var nodeWidth = 20;
        var canvasWidth = 1100;
        var canvasHeight = 400;

        var minX = this.minX,
            maxX = this.maxX,
            minY = this.minY,
            maxY = this.maxY,
            json = jobs.map(function(n) {
          var x = n.x,
              y = n.y;

          //get bounding box
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

        viz = new $jit.ST({
          injectInto: 'dagView',
          width: canvasWidth,
          height: canvasHeight,
          Navigation: {
            enable: true,
            panning: 'avoid nodes',
            zooming: 10 //zoom speed. higher is more sensible
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
            type: 'arrow',
            color: '#23A4FF',
            lineWidth: 0.8
          },
          //Native canvas text styling
          Label: {
            type: 'HTML',
            size: 12,
            style: 'bold'
          },
          //Enable tips
          Tips: {
            enable: true,
            //add positioning offsets
            offsetX: 20,
            offsetY: 20,
            //implement the onShow method to
            //add content to the tooltip when a node
            //is hovered
            onShow: function(tip, node, isLeaf, domElement) {
              var whiteList = ['aliases', 'features', 'jobId'],
                  data = node.data,
                  html = "<div class=\"tip-title\">" + node.name
                + "</div><div class=\'closetip\'>&#10006;</div><div class=\"tip-text\"><ul>";

              for (var k in data) {
                if (~whiteList.indexOf(k)) {
                  if (k == 'jobId') {
                    html += "<li><b>" + k + "</b>: <a href=\"http://hadoop-dw-jt.smf1.twitter.com:50030/jobdetails.jsp?jobid=" +
                      data[k] + "\" target=\"__blank\">" + data[k] + "</a></li>";
                  } else {
                    html += "<li><b>" + k + "</b>: " + data[k] + "</li>";
                  }
                }
              }

              tip.innerHTML =  html + "</ul></div>";
            }
          },
          // Add text to the labels. This method is only triggered
          // on label creation and only for DOM labels (not native canvas ones).
          onCreateLabel: function(domElement, node){
            domElement.innerHTML = node.name;
            var style = domElement.style;
            style.fontSize = "0.8em";
            style.color = "black";
            style.width = nodeWidth + 'px';

            domElement.addEventListener('click', function(e) {
              viz.tooltipPinned = !viz.tooltipPinned;
              viz.tips.tip.classList.toggle('pinned');
            }, false);
          },
          // Change node styles when DOM labels are placed
          // or moved.
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
        // load JSON data.
        viz.loadJSON(json);

        var diffX = maxX - minX,
            diffY = maxY - minY;

        viz.graph.eachNode(function(n) {
          n.pos.setc(diffX ? ((n.data.x - minX) / (maxX - minX) * canvasWidth - canvasWidth / 2 - 2) : (n.data.x - canvasWidth / 2),
                     diffY ? ((n.data.y - minY) / (maxY - minY) * canvasHeight - canvasHeight / 2) : n.data.y);

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

        //var diffX = maxX - minX,
            //diffY = maxY - minY;

        //var xScale = diffX > canvasWidth ? (diffX - canvasWidth) : 0,
            //yScale = diffY > canvasHeight ? (diffY - canvasHeight) : 0;

        //if (xScale || yScale) {
          //if (xScale > yScale) {
            //viz.canvas.scale(canvasWidth / diffX, canvasWidth / diffX);
          //} else {
            //viz.canvas.scale(canvasHeight / diffY, canvasHeight / diffY);
          //}
        //}

        this.extendViz(viz);

        viz.plot();
        // end

    },

    extendViz: function(viz) {
      //Add pinned tooltip behavior
      var tips = viz.tips,
          onMouseMove = tips.onMouseMove,
          onMouseOut  = tips.onMouseOut,
          onMouseOver = tips.onMouseOver,
          wrapper = function (cond, fn) {
            return function() {
              if (cond.apply(this, arguments)) {
                return fn.apply(this, arguments);
              }
            };
          },
          cond = function() {
            return !viz.tooltipPinned;
          },
          hidetip = function(e) {
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
    },

    refreshDisplay: function(event, data) {
      if (!viz) return;

      var type = event.type,
          id = data.job.name,
          n = viz.graph.getNode(id);

      if (n) {
        var label = viz.fx.labels.getLabel(n.id),
            update = false;
        label.className = 'node ' + type;

        if (type == 'JOB_FINISHED') {
          n.eachAdjacency(function(a) {
            a.setData('color', '#aaa');
          });
          update = true;
        } else if (type == 'JOB_FAILED') {
          n.eachAdjacency(function(a) {
            a.setData('color', '#c00');
          });
          update = true;
        }

        if (update) {
          viz.fx.plot();
        }
      }
      console.log(data, n, type);
    }
  };
};
var dagView = $.extend({}, new AMBROSE.chordView(), new AMBROSE.dagView());
