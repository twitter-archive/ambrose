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
 * This module defines the StatusMessage view which displays a single status line which tracks
 * Workflow events.
 */
define(['jquery', '../core', './core'], function($, Ambrose, View) {
  // StatusMessage ctor
  var StatusMessage = View.StatusMessage = function(workflow, container) {
    return new View.StatusMessage.fn.init(workflow, container);
  };

  /**
   * StatusMessage prototype.
   */
  StatusMessage.fn = StatusMessage.prototype = {
    /**
     * Constructor.
     *
     * @param workflow the Workflow instance to bind to.
     * @param container the DOM element in which to render the view.
     */
    init: function(workflow, container) {
      var span = $('<span class="message">').appendTo($('<div>Status:&nbsp;</div>').appendTo($(container).empty()));
      workflow.on('workflowProgress', function() { span.text('Workflow progress'); });
      workflow.on('workflowComplete', function() { span.text('Workflow complete'); });
      workflow.on('loadingJobs', function() { span.text('Loading jobs'); });
      workflow.on('jobsLoaded', function() { span.text('Jobs loaded'); });
      workflow.on('eventPollingStarted', function() { span.text('Event polling started'); });
      workflow.on('eventPollingStopped', function() { span.text('Event polling stopped'); });
      workflow.on('jobStarted', function() { span.text('Job started'); });
      workflow.on('jobProgress', function() { span.text('Job progress'); });
      workflow.on('jobComplete', function() { span.text('Job complete'); });
      workflow.on('jobFailed', function() { span.text('Job failed'); });
    },
  };

  // bind prototype to ctor
  StatusMessage.fn.init.prototype = StatusMessage.fn;
  return StatusMessage;
});
