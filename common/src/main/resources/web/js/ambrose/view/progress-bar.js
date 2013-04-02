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
 * This module defines the ProgressBar view which generates a progress bar for Workflow progress.
 */
define(['jquery', '../core', './core'], function($, Ambrose, View) {
  // ProgressBar ctor
  var ProgressBar = View.ProgressBar = function(workflow, container) {
    return new View.ProgressBar.fn.init(workflow, container);
  };

  /**
   * ProgressBar prototype.
   */
  ProgressBar.fn = ProgressBar.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param container the DOM element in which to render the view.
     */
    init: function(workflow, container) {
      this.workflow = workflow;
      this.container = $(container).empty();
      this.bar = $('<div class="bar">')
        .appendTo($('<div class="progress ambrose-view-progress-bar">').appendTo(this.container))
        .css('width', '0');
      var self = this;
      workflow.on('workflowProgress', function(event, data) {
        self.bar.css('width', data + '%');
      });
    },
  };

  // bind prototype to ctor
  ProgressBar.fn.init.prototype = ProgressBar.fn;
  return ProgressBar;
});
