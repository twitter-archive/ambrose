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
 * Ambrose module "chart" provides base class for charts.
 */
(function($, ambrose) {
  var chart = ambrose.chart = function(ui, divName, tabName) {
    return new ambrose.chart.fn.init(ui, divName, tabName);
  }

  chart.fn = chart.prototype = {
    init: function(ui, divName, tabName) {
      this.ui = ui;
      this.divName = divName;
      this.tabName = tabName;
      if (ui == null) return;
      var self = this;
      ui.bind('dagLoaded', function(event, data) {
        self.addDiv();
        self.initChart(data.jobs);
      });
      ui.bind('jobSelected JOB_STARTED JOB_FINISHED JOB_FAILED', function(event, data) {
        self.refresh(event, data);
      });
    },

    addDiv: function() {
      $('#vizGroup').append('<div class="tab-pane viz-pane" id="' + this.divName + '"></div>');
      $('#vizTabs').append('<li><a href="#' + this.divName + '" data-toggle="tab">' + this.tabName + '</a></li>');
    },
  };

  chart.fn.init.prototype = chart.fn;

}(jQuery, AMBROSE));
