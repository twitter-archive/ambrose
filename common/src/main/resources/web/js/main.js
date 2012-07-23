require([
  'jquery',
  'bootstrap',
  'ambrose',
  'ambrose-ui',
  'ambrose-util',
  'ambrose-detail',
  'ambrose-table',
  'ambrose-chart',
  'ambrose-chord',
  'ambrose-dag'
], function ($, bs, ambrose) {
  var ui = ambrose.ui();
  var detail = ambrose.detail(ui);
  var table = ambrose.table(ui);
  var chord = ambrose.chord(ui);
  var dag = ambrose.dag(ui);

  $(document).ready(function() {
    ui.load();
  });
});
