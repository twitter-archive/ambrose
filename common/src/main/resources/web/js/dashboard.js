requirejs.config(REQUIREJS_CONFIG);

require(['lib/jquery', 'ambrose', 'lib/bootstrap'], function ($, Ambrose) {
  $(document).ready(function() {
    Ambrose.Dashboard();
  });
});
