requirejs.config({
  shim: {
    'uri': { deps: ['jquery'], exports: 'URI' },
    'bootstrap': { deps: ['jquery'], exports: 'bootstrap' },
    'd3': { deps: ['jquery'], exports: 'd3' },
    'colorbrewer': { deps: [], exports: 'colorbrewer' },
  }
});

require(['jquery', 'ambrose'], function($, Ambrose) {
  $(document).ready(function() {
    console.info('Creating default workflow');
    var workflow = Ambrose.Workflow();
    var statusMessage = Ambrose.Views.StatusMessage(workflow, $('#ambrose-views-status-message'));
    var progressBar = Ambrose.Views.ProgressBar(workflow, $('#ambrose-views-progress-bar'));
    var table = Ambrose.Views.Table(workflow, $('#ambrose-views-table'));
    var chord = Ambrose.Views.Chord(workflow, $('#ambrose-views-chord'));
    workflow.loadJobs();
    workflow.startEventPolling();
  });
});
