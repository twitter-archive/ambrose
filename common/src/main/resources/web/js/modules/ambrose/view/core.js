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
 * This module defines the Ambrose.View namespace in which other view modules are registered.
 */
define(['lib/jquery', 'lib/d3', 'lib/colorbrewer', '../core'], function(
  $, d3, colorbrewer, Ambrose
) {
  var pending = d3.rgb(0, 0, 0);
  var running = d3.rgb(98, 196, 98).brighter();
  var complete = d3.rgb('#eee');
  var failed = d3.rgb(196, 98, 98);
  var selected = d3.rgb(98, 98, 196).brighter();
  var mouseover = selected.brighter();
  var nodeEdgeDefault = d3.rgb(170, 170, 170);
  var nodeEdgeScaled = d3.rgb(170, 170, 170).darker();

  return Ambrose.View = {
    Theme: {
      colors: {
        pending: pending,
        running: running,
        complete: complete,
        failed: failed,
        selected: selected,
        mouseover: mouseover,
        nodeEdgeDefault: nodeEdgeDefault,
        nodeEdgeScaled: nodeEdgeScaled
      },
      palettes: {
        queued: colorbrewer.Greys,
        complete: colorbrewer.Greens,
        failed: colorbrewer.Reds
      }
    }
  };
});
