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
    var progressBar = Ambrose.View.ProgressBar(workflow, $('#ambrose-view-progress-bar'));
    var graph = Ambrose.View.Graph(workflow, $('#ambrose-view-graph'));
    var table = Ambrose.View.Table(workflow, $('#ambrose-view-table'));
    var scriptview = Ambrose.View.ScriptView(workflow);

    // install workflow actions
    var workflowDropdown = $('#workflow-dropdown');
    var playbackAction = $('<a href="#">')
      .appendTo($('<li>').appendTo(workflowDropdown));

    var showScriptAction = $('#showScript');
    showScriptAction.toggleClass("hidden", true);
    showScriptAction.text('Show Script');
    showScriptAction.click(function(event) {
      $("#scriptDiv").toggleClass('hidden', false);
    });

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