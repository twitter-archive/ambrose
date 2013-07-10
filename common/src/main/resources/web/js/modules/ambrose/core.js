/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/**
 * This module defines the core Ambrose object and utility methods. Submodules add functionality.
 */
define(['lib/jquery'], function($) {
  // capitalize string
  String.prototype.capitalize = function() {
    return this.charAt(0).toUpperCase() + this.slice(1).toLowerCase();
  };

  // pattern for separator char + alpha
  var separatorAlphaPattern = /[-_ ]+([a-z])/ig;

  // util function used by camelCase
  function toUpper(all, match) {
    return match.toUpperCase();
  }

  // camel case string
  String.prototype.camelCase = function() {
    return this.replace(separatorAlphaPattern, toUpper);
  };

  // find max value within array
  Array.prototype.max = function() {
    return Math.max.apply(Math, this);
  };

  // find min value within array
  Array.prototype.min = function() {
    return Math.min.apply(Math, this);
  };

  // remove value from array
  Array.prototype.remove = function(object) {
    var i = $.inArray(object, this);
    if (i < 0) return;
    return this.splice(i, 1);
  };

  // core Ambrose object
  return {};
});
