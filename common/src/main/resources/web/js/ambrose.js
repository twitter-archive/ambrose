define([], function() {

  // add utility functions to built-in javascript classes

  Array.prototype.max = function() {
    return Math.max.apply(Math, this);
  };

  Array.prototype.min = function() {
    return Math.min.apply(Math, this);
  };

  Array.prototype.remove = function(object) {
    var i = $.inArray(object, this);
    if (i < 0) return;
    return this.splice(i, 1);
  };

  // empty ambrose object
  return {};

});
