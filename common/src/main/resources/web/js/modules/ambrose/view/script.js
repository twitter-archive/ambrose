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

      // Create script button and hide it by default.
      var showScriptAction = $('#showScript');
      showScriptAction.toggleClass("hidden", true);
      showScriptAction.text('Show Script');
      showScriptAction.click(function(event) {
        $(".scriptDiv").toggleClass('hidden', false);
      });

      // Update the script view if needed.
      workflow.on('jobPolled', function(event, data) {
        if (data && data.job && data.job.runtime == "pig") {
          // Unhide Script Button
          showScriptAction.toggleClass("hidden", false);
          self.updateScript(data);
        }
      });

      // Handle mouse interaction.
      workflow.on('jobMouseOver', function(event, job, prev, hoveredJob, selectedJob) {
        self.clearScript();
        if ((hoveredJob && hoveredJob.runtime == "pig")
            || (selectedJob && selectedJob.runtime == "pig")) {
          self.highlightScript(hoveredJob, 'mouseOver', false);
          self.highlightScript(selectedJob, 'mouseClick', false);
        }
      });

      workflow.on('jobSelected', function(event, job, prev, hoveredJob, selectedJob) {
        self.clearScript();
        if ((hoveredJob && hoveredJob.runtime == "pig")
            || (selectedJob && selectedJob.runtime == "pig")) {
          self.highlightScript(hoveredJob, 'mouseOver', false);
          self.highlightScript(selectedJob, 'mouseClick', true);
        }
      });
    },

    /**
     * Create the div used for script view.
     */
    createScriptDiv : function() {
      var self = this;
      self.scriptDiv = $('<div />').appendTo('body');
      self.scriptDiv.addClass("modal hidden scriptDiv");
      var $scriptContent = $('<div />').appendTo(self.scriptDiv);
      $scriptContent.addClass('scriptContent');

      self.scriptDiv.draggable({
        handle: '.scriptDivTitle'
      }).resizable({
        handles: 'n, e, s, w, ne, se, sw, nw'
      });

      var $titleEl = $('<div />').appendTo($scriptContent);
      $titleEl.addClass('modal-header scriptDivTitle');
      $titleEl.html('<span class="scriptPopoverName">' + "Pig Script" + '</span>');

      var $scriptCloseBtn = $('<button />').appendTo($titleEl);
      $scriptCloseBtn.addClass('close scriptTitleCloseBtn');
      $scriptCloseBtn.html('&times;');
      $scriptCloseBtn.click(function() {
        $(".scriptDiv").toggleClass('hidden', true);
      });

      var $scriptRefreshBtn = $('<button />').appendTo($titleEl);
      $scriptRefreshBtn.addClass('close scriptTitleRefreshBtn');
      $scriptRefreshBtn.html('&#8635;');
      $scriptRefreshBtn.click(function() {
        self.clearScript();
        if (self.scriptDiv && self.scriptDiv.find('.scriptLoaded').length > 0) {
          self.scriptDiv.find('.scriptDivBody').eq(0).scrollTop(0);
        }
      });

      var $bodyEl = $('<div />').appendTo($scriptContent);
      $bodyEl.addClass('scriptDivBody');
      $bodyEl.html("Script is not ready, please refresh the page.");
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
        var $scriptNameEl = self.scriptDiv.find('.scriptPopoverName').eq(0);
        $scriptNameEl.html(data.job.configuration["mapred.job.name"]);

        // Update the script body section with the actual script.
        var $scriptBodyEl = self.scriptDiv.find('.scriptDivBody').eq(0);
        $scriptBodyEl.addClass("scriptLoaded");
        $scriptBodyEl.html(self.renderScript(data.job.configuration["pig.script"]));
      }
    },

    /**
     * Create a table to display the script.
     */
    renderScript : function(script) {
      var lineCounter = 1;

      script = '<table><tr class="scriptLine">'
          + '<td class="lineNumber">' + lineCounter + '</td>'
          + '<td class="aliasM">&nbsp;</td>'
          + '<td class="aliasC">&nbsp;</td>'
          + '<td class="aliasR">&nbsp;</td><td>' + script;

      while (script.indexOf("<newLine>") != -1) {
        lineCounter++;
        script = script.replace('<newLine>', '</td></tr><tr class="scriptLine">'
            + '<td class="lineNumber">' + lineCounter + '</td>'
            + '<td class="aliasM">&nbsp;</td>'
            + '<td class="aliasC">&nbsp;</td>'
            + '<td class="aliasR">&nbsp;</td><td>');
      }

      return script + "</td></tr></table>";
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
        var minLineNum = 10000; // Should be a large enough initial value.

        for (var i = 0; i < aliases.length; i++) {
          var group = aliases[i].substring(0, 1);
          var lines = aliases[i].match(/\[(.*?)\,/g);
          if (lines != null) {
            for (var j = 0; j < lines.length; j++) {
              var lineNum = Number(lines[j].substring(1, lines[j].length - 1));

              var $selectedLine = self.scriptDiv.find('.scriptLine').eq(lineNum - 1);
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

              if (lineNum < minLineNum) { minLineNum = lineNum; }
            }
          }
        }

        if (scrollTo && self.scriptDiv.find(".scriptLine").eq(minLineNum - 1).length > 0) {
          var scrollValue = self.scriptDiv.find(".scriptLine").eq(minLineNum - 1).offset().top
              - self.scriptDiv.find(".scriptLine").eq(0).offset().top;
          $('.scriptDivBody').scrollTop(scrollValue);
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
          self.scriptDiv.find(".alias" + aliasType).eq(lineNum - 1).html(aliasType);
        } else if (!job.mouseover) {
          self.scriptDiv.find(".alias" + aliasType).eq(lineNum - 1).html("&nbsp;");
        }
      }
    },

    /**
     * Clear all the highlighting of the script.
     */
    clearScript : function() {
      var self = this;
      if (self.scriptDiv) {
        self.scriptDiv.find('.scriptLine').toggleClass("selected", false);
        self.scriptDiv.find('.scriptLine').toggleClass("hovered", false);
        self.scriptDiv.find('.lineNumber').toggleClass("selected", false);
        self.scriptDiv.find(".aliasM").html("&nbsp;");
        self.scriptDiv.find(".aliasC").html("&nbsp;");
        self.scriptDiv.find(".aliasR").html("&nbsp;");
      }
    }
  };

  // Bind prototype to ctor
  Script.fn.init.prototype = Script.fn;
  return Script;
});
