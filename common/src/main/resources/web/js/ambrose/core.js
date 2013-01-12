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
 * This module defines the core Ambrose object and utility methods. Submodules add functionality.
 */
define(['jquery'], function($) {
  // capitalize string
  String.prototype.capitalize = function() {
    return this.charAt(0).toUpperCase() + this.slice(1);
  };

  // pattern for separator char + alpha
  var rSepAlpha = /[-_ ]+([a-z])/ig;

  // util function used by camelCase
  var fToUpper = function(all, match) {
    return match.toUpperCase();
  };

  // camel case string
  String.prototype.camelCase = function() {
    return this.replace(rSepAlpha, fToUpper);
  };

  // find max value within array
  Array.prototype.max = function() {
    return Math.max.apply(Math, this);
  };

  // find min value within array
  Array.prototype.min = function() {
    return Math.min.apply(Math, this);
  };

  // remove value from array
  Array.prototype.remove = function(object) {
    var i = $.inArray(object, this);
    if (i < 0) return;
    return this.splice(i, 1);
  };

  // core Ambrose object containing utility methods
  return {
    isNull: function(v) { return v == null; },
    notNull: function(v) { return v != null; },

    /**
     * @param array values to join.
     * @return string containing array values delimited by ', '.
     */
    commaDelimit: function(array) {
      if (array == null) return '';
      return array.join(', ');
    },

    /**
     * @param totalTasks total number of tasks.
     * @param taskProgress number in [0, 1].
     * @return formatted message.
     */
    taskProgressMessage: function(totalTasks, taskProgress) {
      if (totalTasks == null || taskProgress == null) return '';
      return totalTasks + ' (' + Math.round(Number(taskProgress) * 100, 0) + '%)';
    },
  };
});
