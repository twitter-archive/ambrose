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

    var diagrams = $('#diagrams');
    var chordContainer = $('#ambrose-views-chord');
    var graphContainer = $('#ambrose-views-graph');

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

    var chord = Ambrose.Views.Chord(workflow, chordContainer);
    var graph = Ambrose.Views.Graph(workflow, graphContainer);
    var table = Ambrose.Views.Table(workflow, $('#ambrose-views-table'));
    workflow.loadJobs();
    workflow.startEventPolling();
  });
});
