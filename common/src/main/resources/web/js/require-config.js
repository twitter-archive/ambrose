var REQUIREJS_CONFIG = {
  baseUrl: '/js/modules',
  url__Args: 'bust=' + new Date().getTime(),
  paths: {
    lib: '/js/lib',
  },
  shim: {
    'lib/jquery': { exports: 'jQuery' },
    'lib/uri': { deps: ['lib/jquery'], exports: 'uri' },
    'lib/bootstrap': { deps: ['lib/jquery'], exports: 'bootstrap' },
    'lib/d3': { deps: ['lib/jquery'], exports: 'd3' },
    'lib/colorbrewer': { exports: 'colorbrewer' },
  }
};
