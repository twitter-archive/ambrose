requirejs.config({
  shim: {
    'bootstrap': { deps: ['jquery'], exports: 'bootstrap' },
    'uri': { deps: ['jquery'], exports: 'URI' },
    'd3': { deps: ['jquery'], exports: 'd3' },
    'colorbrewer': { deps: [], exports: 'colorbrewer' },
  }
});

require(['jquery', 'ambrose', 'bootstrap'], function ($, Ambrose) {
  $(document).ready(function() {
    Ambrose.Dashboard();
  });
});
