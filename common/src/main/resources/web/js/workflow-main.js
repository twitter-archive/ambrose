requirejs.config({
  shim: {
    'bootstrap': { deps: ['jquery'], exports: 'bootstrap' },
    'uri': { deps: ['jquery'], exports: 'URI' },
    'd3': { deps: ['jquery'], exports: 'd3' },
    'colorbrewer': { deps: [], exports: 'colorbrewer' },
  }
});

require(['jquery', 'ambrose', 'bootstrap'], function($, Ambrose) {
  $(document).ready(function() {
    console.info('Creating default workflow');
    var workflow = Ambrose.Workflow();
    var progressBar = Ambrose.View.ProgressBar(workflow, $('#ambrose-view-progress-bar'));
    var graph = Ambrose.View.Graph(workflow, $('#ambrose-view-graph'));
    var table = Ambrose.View.Table(workflow, $('#ambrose-view-table'));
    workflow.loadJobs();
    workflow.startEventPolling();
  });
});
