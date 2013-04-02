requirejs.config({
  shim: {
    'uri': { deps: ['jquery'], exports: 'URI' },
    'bootstrap': { deps: ['jquery'], exports: 'bootstrap' },
    'd3': { deps: ['jquery'], exports: 'd3' },
    'colorbrewer': { deps: [], exports: 'colorbrewer' },
  }
});

require(['jquery', 'ambrose', 'bootstrap'], function($, Ambrose) {
  $(document).ready(function() {
    console.info('Creating default workflow');
    var workflow = Ambrose.Workflow();
    var progressBar = Ambrose.View.ProgressBar(workflow, $('#ambrose-view-progress-bar'));

    var diagrams = $('#diagrams');
    var chordContainer = $('#ambrose-view-chord');
    var graphContainer = $('#ambrose-view-graph');

    var height = diagrams.height();
    var width = diagrams.width();

    // who needs responsive css?
    if (width <= 1024) {
      chordContainer.css('display', 'none');
      graphContainer.width(width).height(250);
    } else {
      chordContainer.width(height);
      graphContainer.width(width - height);
    }

    var chord = Ambrose.View.Chord(workflow, chordContainer);
    var graph = Ambrose.View.Graph(workflow, graphContainer);
    var table = Ambrose.View.Table(workflow, $('#ambrose-view-table'));
    workflow.loadJobs();
    workflow.startEventPolling();
  });
});
