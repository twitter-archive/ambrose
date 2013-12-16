requirejs.config(REQUIREJS_CONFIG);

require([
  'lib/jquery', 'lib/uri', 'ambrose', 'lib/bootstrap'
], function($, URI, Ambrose) {
  $(document).ready(function() {
    // parse uri params
    var uri = new URI(window.location.href);
    var params = uri.search(true);

    // initialize workflow and views
    console.info('Creating default workflow');
    var workflow = Ambrose.Workflow();
    var $graphContainer = $('#ambrose-view-graph');
    // Rescale the edges based on the option chosen, default option.
    workflow.rescaleOption = "hdfsBytesWritten";

    var progressBar = Ambrose.View.ProgressBar(workflow, $('#ambrose-view-progress-bar'));
    var graph = Ambrose.View.Graph(workflow, $graphContainer);
    var table = Ambrose.View.Table(workflow, $('#ambrose-view-table'));
    var nodePopover = Ambrose.View.NodePopover(workflow, $graphContainer);
    var edgePopover = Ambrose.View.EdgePopover(workflow, $graphContainer, graph);
    var script = Ambrose.View.Script(workflow);

    // install workflow actions
    var workflowDropdown = $('#workflow-dropdown');
    var playbackAction = $('<a href="#">')
      .appendTo($('<li>').appendTo(workflowDropdown));

    // load jobs and poll for events
    if (params.replay) {
      delete params.replay;
      uri.search(params);
      playbackAction.text('Jump to end').attr('href', uri.toString());
      workflow.replay();
    } else {
      params.replay = true;
      uri.search(params);
      playbackAction.text('Replay').attr('href', uri.toString());
      workflow.jumpToEnd();
    }
  });

});
