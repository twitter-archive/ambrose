/*
 Copyright 2015 Twitter, Inc.

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
 * PathExpression is a utility for defining complex accessors for deeply nested fields of Objects.
 */
define(['lib/jquery', './core'], function($, Ambrose) {
  var defaultControlChars = {
    escape: '\\',
    elementSeparator: '.',
    alternateSeparator: '|',
    groupBegin: '(',
    groupEnd: ')',
  };

  function highlight(s, i) {
    var c = (0 <= i && i < s.length) ? s[i] : ' ';

    if (i < 0) i = 0;
    if (i > s.length) i = s.length;

    var b = i - 10;
    var e = i + 11;
    if (b < 0) b = 0;
    if (e > s.length) e = s.length;

    return s.slice(b, i) + '[' + c + ']' + s.slice(i + 1, e);
  }

  /**
   * Parses a path string of the form:
   *
   * path ::= element ( '.' element )*
   *
   * element ::= alternate ( '|' alternate )*
   *
   * alternate ::= group | field
   *
   * group ::= '(' path ')'
   *
   * field ::= ( escaped_char | char )*
   *
   * escaped_char ::= '\' any_char
   *
   * char ::= ! control_char
   *
   * control_char ::= '\' | '.' | '|' | '(' | ')'
   *
   * @param s path expression.
   * @param controlChars control character overrides.
   * @returns {{type: string, elements: *, begin: number, end: number}}
   */
  function parse(s, controlChars) {
    // init control characters
    controlChars = $.extend(true, {}, defaultControlChars, controlChars);

    // current character index within path expression
    var i = 0;

    // chars of current field
    var chars = [];

    // current field, element, path
    var field = FieldExpression(0);
    var element = ElementExpression();
    var path = new PathExpression.fn.init();

    function pushField() {
      if (chars.length > 0) {
        field.value = chars.join('');
        field.end = i;
        element.push(field);
        chars = [];
      }
      field = FieldExpression(i + 1);
    }

    function pushElement() {
      pushField();
      if (!element.isEmpty()) {
        element.end = i;
        path.push(element);
      }
      element = ElementExpression(i + 1);
    }

    PATH_CHARACTERS: for (; i < s.length; ++i) {
      switch (s[i]) {
        case controlChars.escape:
          // add next char to field
          i += 1;
          if (i >= s.length) {
            throw new SyntaxError("Trailing escape char found at end of path expression '" + s + "': " + highlight(s, i));
          }
          chars.push(s[i]);
          break;

        case controlChars.elementSeparator:
          // push current element into path
          pushElement();
          break;

        case controlChars.alternateSeparator:
          // push current field into element
          pushField();
          break;

        case controlChars.groupBegin:
          // ensure current field is empty
          if (chars.length > 0) {
            debugger;
            throw new SyntaxError("Group begin '(' found within field at index " + i + " of path expression '" + s + "': " + highlight(s, i));
          }

          // recurse and update state
          var begin = i + 1;
          var group = parse(s.slice(begin), controlChars);
          if (!group.isEmpty()) {
            group.begin += begin;
            group.end += begin;
            element.push(group);
          }

          // confirm trailing ')'
          i = group.end;
          if (i >= s.length || s[i] != ')') {
            throw new SyntaxError("Missing group end ')' at index " + i + " of path expression '" + s + "': " + highlight(s, i));
          }
          break;

        case controlChars.groupEnd:
          // break and return
          break PATH_CHARACTERS;

        default:
          // add char to current field
          chars.push(s[i]);
          break;
      }
    }

    // take care of trailing element
    pushElement();

    // return path
    path.end = i;
    return path;
  }

  var FieldExpression = function(begin) {
    return new FieldExpression.fn.init(begin);
  };

  FieldExpression.fn = FieldExpression.prototype = {
    init: function(begin) {
      this.begin = begin;
    },

    apply: function(obj) {
      if (obj == null) return null;
      var value = obj[this.value];
      if (value == null) return null;
      return {
        field: this,
        value: value,
      };
    },
  };

  FieldExpression.fn.init.prototype = FieldExpression.fn;

  var ElementExpression = function(begin) {
    return new ElementExpression.fn.init(begin);
  };

  ElementExpression.fn = ElementExpression.prototype = {
    init: function(begin) {
      this.alternates = [];
      this.begin = begin;
    },

    isEmpty: function() {
      return this.alternates.length == 0;
    },

    push: function(alternate) {
      this.alternates.push(alternate);
      alternate.parent = this;
    },

    apply: function(obj) {
      var result = null;
      for (var i = 0; result == null && i < this.alternates.length; ++i) {
        result = this.alternates[i].apply(obj);
      }
      return result;
    },
  };

  ElementExpression.fn.init.prototype = ElementExpression.fn;

  // class function which invokes parse
  var PathExpression = Ambrose.PathExpression = function(s, controlChars) {
    return parse(s, controlChars);
  };

  // expose parse as static function of class
  PathExpression.parse = parse;

  // define class prototype
  PathExpression.fn = PathExpression.prototype = {
    init: function() {
      this.elements = [];
      this.begin = 0;
    },

    isEmpty: function() {
      return this.elements.length == 0;
    },

    push: function(element) {
      this.elements.push(element);
      element.parent = this;
    },

    apply: function(obj) {
      var result = null;
      for (var i = 0; obj != null && i < this.elements.length; ++i) {
        result = this.elements[i].apply(obj);
        if (result == null) break;
        obj = result.value;
      }
      return result;
    },

    value: function(obj) {
      var result = this.apply(obj);
      if (result == null) return null;
      return result.value;
    },
  };

  // link ctor prototype w/ class prototype
  PathExpression.fn.init.prototype = PathExpression.fn;

  return PathExpression;

});
