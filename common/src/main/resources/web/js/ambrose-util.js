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
 * Ambrose module "util" containing static helper functions.
 */
(function($, d3, ambrose) {
  var util = ambrose.util = {};

  $.extend(util, {
    comma_join: function(array) {
      if  (array != null) {
        return array.join(', ');
      }
      return '';
    },

    task_progress_string: function(totalTasks, taskProgress) {
      if (totalTasks == null || taskProgress == null) {
        return '';
      }
      return totalTasks + ' (' + d3.round(taskProgress * 100, 0) + '%)';
    },

    value: function(value) {
      if (value == null) {
        return '';
      }
      return value;
    },
  });
}(jQuery, d3, AMBROSE));
