require(['jquery',
         'bootstrap',
         'ambrose',
         'ambrose-ui',
         'ambrose-util',
         'ambrose-detail',
         'ambrose-table',
         'ambrose-chart',
         'ambrose-chord',
         'ambrose-dag'],

         function ($, bs, AMBROSE) {

  var ui = AMBROSE.ui();
  var detail = AMBROSE.detail(ui);
  var table = AMBROSE.table(ui);
  var chord = AMBROSE.chord(ui);
  var dag = AMBROSE.dag(ui);

  $(document).ready(function() {
    ui.load();
  });
});
