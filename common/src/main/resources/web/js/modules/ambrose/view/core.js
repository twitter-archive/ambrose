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
  return Ambrose.View = {
    Theme: {
      colors: {
        running: d3.rgb(98, 196, 98).brighter(),
        complete: d3.rgb(98, 196, 98),
        failed: d3.rgb(196, 98, 98),
        mouseover: d3.rgb(98, 98, 196).brighter().brighter(),
        selected: d3.rgb(98, 98, 196).brighter(),
      },
      palettes: {
        queued: colorbrewer.Greys,
        complete: colorbrewer.Greens,
        failed: colorbrewer.Reds,
      },
    }
  };
});
