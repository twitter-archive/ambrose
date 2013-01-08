requirejs.config({
  shim: {
    'uri': { deps: ['jquery'], exports: 'URI' },
    'bootstrap': { deps: ['jquery'], exports: 'bootstrap' },
    'd3': { deps: ['jquery'], exports: 'd3' },
  }
});

require(['jquery', 'ambrose'], function($, Ambrose) {
  $(document).ready(function() {
    console.info('Creating default workflow');
    var workflow = Ambrose.Workflow();
    var table = Ambrose.Views.Table(workflow, $('#ambrose-views-table'));
    workflow.loadJobs();
    workflow.startEventPolling();
  });
});
