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
 * This module defines the functions related to pig-script view. It helps to render pig script and
 * display it properly.
 */
define(['lib/jquery', '../core', './core', 'lib/jquery-ui'], function($, Ambrose, View) {
  // Script ctor
  var Script = View.Script = function(workflow) {
    return new View.Script.fn.init(workflow);
  };

   /**
   * Script prototype.
   */
  Script.fn = Script.prototype = {
    /**
     * Constructor.
     * @param workflow the Workflow instance to bind to.
     */
    init: function(workflow) {
      var self = this;
      self.createScriptDiv();

      // Update the script view if needed.
      workflow.on('jobPolled', function(event, data) {
        if (data && data.job && data.job.runtime == "pig") {
          // Unhide Script Button
          self.showScriptAction.toggleClass("hidden", false);
          self.updateScript(data);
        }
      });

      // Handle mouse interaction.
      workflow.on('jobMouseOver', function(event, job, prev) {
        self.unhighlightScript();
        if ((job && job.runtime == "pig") || (prev && prev.runtime == "pig")) {
          self.highlightScript(job, 'mouseOver', false);
          self.highlightScript(workflow.current.selected, 'mouseClick', false);
        }
      });

      workflow.on('jobSelected', function(event, job, prev) {
        self.unhighlightScript();
        if ((job && job.runtime == "pig") || (prev && prev.runtime == "pig")) {
          self.highlightScript(workflow.current.mouseover, 'mouseOver', false);
          self.highlightScript(job, 'mouseClick', true);
        }
      });
    },

    /**
     * Create the div used for script view.
     */
    createScriptDiv : function() {
      var self = this;

      self.scriptDiv = $('<div />').appendTo('body');
      self.scriptDiv.addClass("modal hidden ambrose-view-script");

      // Create Show Script button on the page.
      if ($('#ambrose-navbar li.ambrose-view-script-show a').length == 0) {
        self.showScriptAction = $("<a />").appendTo($('<li class="ambrose-view-script-show"/>').appendTo($('#ambrose-navbar ul.nav:eq(0)')));
        self.showScriptAction.toggleClass("hidden", true);
        self.showScriptAction.text('Show Script');
        self.showScriptAction.click(function(event) {
          self.scriptDiv.toggleClass('hidden', false);
        });
      }

      var $scriptContent = $('<div />').appendTo(self.scriptDiv);
      $scriptContent.addClass('scriptContent');

      self.scriptDiv.draggable({
        handle: '.scriptDivTitle'
      }).resizable({
        handles: 'n, e, s, w, ne, se, sw, nw'
      });

      var $titleEl = $('<div />').appendTo($scriptContent);
      $titleEl.addClass('modal-header scriptDivTitle');
      self.scriptNameEl = $('<span />').appendTo($titleEl).addClass('scriptName');
      self.scriptNameEl.html("Script");

      var $scriptCloseBtn = $('<button />').appendTo($titleEl);
      $scriptCloseBtn.addClass('close scriptTitleCloseBtn');
      $scriptCloseBtn.html('&times;');
      $scriptCloseBtn.click(function() {
        self.scriptDiv.toggleClass('hidden', true);
      });

      var $scriptRefreshBtn = $('<button />').appendTo($titleEl);
      $scriptRefreshBtn.addClass('close scriptTitleRefreshBtn');
      $scriptRefreshBtn.html('&#8635;');
      $scriptRefreshBtn.click(function() {
        self.unhighlightScript();
        if (self.scriptDiv && self.scriptDiv.find('.scriptLoaded').length > 0) {
          self.scriptBodyEl.scrollTop(0);
        }
      });

      self.scriptBodyEl = $('<div />').appendTo($scriptContent);
      self.scriptBodyEl.addClass('scriptDivBody');
      self.scriptBodyEl.html("Script is not ready, please refresh the page.");
    },

    /**
     * Render the script view with actual data.
     */
    updateScript : function(data) {
      var self = this;
      if (self.scriptDiv && self.scriptDiv.find('.scriptLoaded').length == 0
          && data.job && data.job.configuration
          && data.job.configuration["pig.script"]) {
        // Update Script Title.
        self.scriptNameEl.html(data.job.configuration["mapred.job.name"]);

        // Update the script body section with the actual script.
        self.scriptBodyEl.addClass("scriptLoaded");
        self.scriptBodyEl.html(self.renderScript(data.job.configuration["pig.script"]));
      }
    },

    /**
     * Create a table to display the script.
     */
    renderScript : function(scriptEncoded) {
      var lineCounter = 1;
      scriptEncoded = scriptEncoded.b64_to_utf8();

      scriptEncoded = '<table><tr class="scriptLine">'
          + '<td class="lineNumber">' + lineCounter + '</td>'
          + '<td class="aliasM">&nbsp;</td>'
          + '<td class="aliasC">&nbsp;</td>'
          + '<td class="aliasR">&nbsp;</td><td>' + scriptEncoded;

      while (scriptEncoded.indexOf("\n") != -1) {
        lineCounter++;
        scriptEncoded = scriptEncoded.replace('\n', '</td></tr><tr class="scriptLine">'
            + '<td class="lineNumber">' + lineCounter + '</td>'
            + '<td class="aliasM">&nbsp;</td>'
            + '<td class="aliasC">&nbsp;</td>'
            + '<td class="aliasR">&nbsp;</td><td>');
      }

      return scriptEncoded + "</td></tr></table>";
    },

    /**
     * Handles the mouse interaction, and highlight the part of the script that corresponds to
     * the alias of the node hovered over.
     * @param job - a node to highlight/unhighlight
     * @param action - hovered or clicked
     * @param scrollTo - scroll to the line only when this is true
     */
    highlightScript : function(job, action, scrollTo) {
      if (!job) return ; // null check
      var self = this;

      if (job.configuration && job.configuration["pig.alias.location"]) {
        // Aliases are in order of M:...C:... R:...
        var aliases = job.configuration["pig.alias.location"].split(/(?=[A-Z]:)/);
        // Initialize the minLineNum to max int.
        var minLineNum = Number.MAX_VALUE;

        for (var i = 0; i < aliases.length; i++) {
          var group = aliases[i].substring(0, 1);
          var lines = aliases[i].match(/\[(.*?)\,/g);
          if (lines != null) {
            for (var j = 0; j < lines.length; j++) {
              var lineNum = Number(lines[j].substring(1, lines[j].length - 1));

              var $selectedLine = self.scriptBodyEl.find('.scriptLine').eq(lineNum - 1);
              // If the line doesn't exist in the script (script not long enough to show this line).
              if (!$selectedLine) { return; }

              if (action == 'mouseOver') {
                $selectedLine.toggleClass('hovered', job.mouseover);
              } else if (action == 'mouseClick') {
                $selectedLine.toggleClass('selected', job.selected);
                $selectedLine.find('.lineNumber').eq(0).toggleClass('selected', job.selected);

                if (group === "C" || group === "M"  || group === "R") {
                  self.setAliasType(group, lineNum, job);
                }
              }

              if (lineNum < minLineNum && lineNum > 0) { minLineNum = lineNum; }
            }
          }
        }

        if (scrollTo && self.scriptBodyEl.find(".scriptLine").eq(minLineNum - 1).length > 0) {
          var scrollValue = self.scriptBodyEl.find(".scriptLine").eq(minLineNum - 1).offset().top
              - self.scriptBodyEl.find(  ".scriptLine").eq(0).offset().top;
          self.scriptBodyEl.scrollTop(scrollValue);
        }
      }
    },

    /**
     * Toggle the alias type on/off for a line based on the mouse action.
     */
    setAliasType : function(aliasType, lineNum, job) {
      var self = this;
      if (self.scriptDiv) {
        if (job.selected) {
          self.scriptBodyEl.find(".alias" + aliasType).eq(lineNum - 1).html(aliasType);
        } else if (!job.mouseover) {
          self.scriptBodyEl.find(".alias" + aliasType).eq(lineNum - 1).html("&nbsp;");
        }
      }
    },

    /**
     * Clear all the highlighting of the script by removing the classes and reset alias to a space.
     */
    unhighlightScript : function() {
      var self = this;
      if (self.scriptDiv) {
        self.scriptBodyEl.find('.scriptLine').toggleClass("selected", false).toggleClass("hovered", false);
        self.scriptBodyEl.find('.lineNumber').toggleClass("selected", false);
        self.scriptBodyEl.find(".aliasM").html("&nbsp;");
        self.scriptBodyEl.find(".aliasC").html("&nbsp;");
        self.scriptBodyEl.find(".aliasR").html("&nbsp;");
      }
    }
  };

  // Bind prototype to ctor
  Script.fn.init.prototype = Script.fn;
  return Script;
});
