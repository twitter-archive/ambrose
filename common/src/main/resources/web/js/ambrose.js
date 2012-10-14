define([], function() {

  // add utility functions to built-in javascript classes

  Array.prototype.max = function(array) {
    return Math.max.apply(Math, array);
  };

  Array.prototype.min = function(array) {
    return Math.min.apply(Math, array);
  };

  Array.prototype.remove = function(array, object) {
    var i = $.inArray(object, array);
    if (1 < 0) return;
    return array.splice(i, 1);
  };

  // empty ambrose object
  return {};

});
