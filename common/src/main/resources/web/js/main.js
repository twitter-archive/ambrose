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
    var progressBar = Ambrose.Views.ProgressBar(workflow, $('#ambrose-views-progress-bar'));
    var chord = Ambrose.Views.Chord(workflow, $('#ambrose-views-chord'));
    var dag = Ambrose.Views.Graph(workflow, $('#ambrose-views-graph'));
    var table = Ambrose.Views.Table(workflow, $('#ambrose-views-table'));
    workflow.loadJobs();
    workflow.startEventPolling();
  });
});
