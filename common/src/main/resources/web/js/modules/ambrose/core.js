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

  String.prototype.b64_to_utf8 = function() {
    return decodeURIComponent(escape(window.atob(this)));
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

  function pad(number) {
    var r = String(number);
    if (r.length === 1) r = '0' + r;
    return r;
  }

  // Add commas to numbers.
  String.prototype.commafy = function () {
    return this.replace(/(^|[^\w.])(\d{4,})/g, function($0, $1, $2) {
        return $1 + $2.replace(/\d(?=(?:\d\d\d)+(?!\d))/g, "$&,");
    });
  };

  Number.prototype.commafy = function () {
    return String(this).commafy();
  };


  Number.prototype.formatTimestamp = function() {
    var date = new Date(this);
    var timezoneOffsetHours = date.getTimezoneOffset() / 60;
    var timezoneSeparator = timezoneOffsetHours >= 0 ? '-' : '+';
    timezoneOffsetHours = Math.abs(timezoneOffsetHours);

    return date.getFullYear() + '-'
    + pad(date.getMonth() + 1) + '-'
    + pad(date.getDate()) + ' '
    + pad(date.getHours()) + ':'
    + pad(date.getMinutes()) + ':'
    + pad(date.getSeconds())
    + ' UTC' + timezoneSeparator + pad(timezoneOffsetHours);
  };

  // core Ambrose object, util methods
  return {
    calculateElapsedTime : function(start, end) {
      var ms = Number(end) - Number(start);

      var d, h, m, s;
      var elapsedTime = "";

      s = Math.floor(ms / 1000);
      m = Math.floor(s / 60);
      s = s % 60;
      h = Math.floor(m / 60);
      m = m % 60;
      d = Math.floor(h / 24);
      h = h % 24;

      if (d != 0) elapsedTime += d + "d ";
      if (h != 0) elapsedTime += h + "h ";
      if (m != 0) elapsedTime += m + "m ";
      if (s != 0) elapsedTime += s + "s ";
      return elapsedTime;
    }
  };
});
