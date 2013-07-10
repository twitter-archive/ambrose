requirejs.config(REQUIREJS_CONFIG);

require(['lib/jquery', 'ambrose', 'lib/bootstrap'], function($, Ambrose) {
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
