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
define(['lib/jquery', '../core', './core'], function($, Ambrose, View) {
  // ScriptView ctor
  var ScriptView = View.ScriptView = function(workflow) {
    return new View.ScriptView.fn.init(workflow);
  };

   /**
   * ProgressBar prototype.
   */
  ScriptView.fn = ScriptView.prototype = {
    /**
     * Constructor.
     * @param workflow the Workflow instance to bind to.
     * @param container the DOM element in which to render the view.
     */
    init: function(workflow) {
      this.params = $.extend(true, {}, View.Theme, null);
      var self = this;
      var colors = this.params.colors;
      self.createScriptDiv(colors);

      // Update the script view if needed.
      workflow.on('jobpolled', function(event, data) {
        if (data && data.job && data.job.runtime == "pig") {
          self.updateScript(data);
        }
      });

      workflow.on('jobMouseOver', function(event, job, prev, selected) {
        if ((job && job.runtime == "pig") || (prev && prev.runtime == "pig")) {
          self.highlineScript(colors, prev, "scriptCancel", false);
          self.highlineScript(colors, selected, "scriptClicked", false);
          self.highlineScript(colors, job, "scriptHovered", false);
        }
      });

      workflow.on('jobSelected', function(event, job, prev) {
        if ((job && job.runtime == "pig") || (prev && prev.runtime == "pig")) {
          self.highlineScript(colors, prev, "scriptCancel", false);
          self.highlineScript(colors, job, "scriptClicked", true);
        }
      });
    },

    /**
     * Create the div used for script view.
     */
    createScriptDiv : function(colors) {
      var self = this;
      $('#scriptDiv').draggable({
        handle: '#scriptDivTitle'
      }).resizable({
        handles: 'n, e, s, w, ne, se, sw, nw'
      });

      var scriptCtn = document.getElementById('scriptContent');
      var titleEl = document.createElement('div');
      titleEl.className = "modal-header";
      titleEl.id = "scriptDivTitle";
      titleEl.innerHTML = '<span id="scriptPopoverName">' + "Pig Script" + '</span>';

      var scriptCloseBtn = document.createElement('button');
      scriptCloseBtn.className = "close";
      scriptCloseBtn.id = "scriptTitleBtn1";
      scriptCloseBtn.innerHTML = "&times;";
      scriptCloseBtn.onclick = function() {
        $("#scriptDiv").toggleClass('hidden', true);
      };

      var scriptRefreshBtn = document.createElement('button');
      scriptRefreshBtn.className = "close";
      scriptRefreshBtn.id = "scriptTitleBtn2";
      scriptRefreshBtn.innerHTML = "&#8635;";
      scriptRefreshBtn.onclick = function() {
        self.clearScript(colors);
        if ($("#scriptDivBody").length > 0) {
          $('#scriptDivBody').scrollTop(0);
        }
      };

      titleEl.appendChild(scriptCloseBtn);
      titleEl.appendChild(scriptRefreshBtn);
      scriptCtn.appendChild(titleEl);

      var bodyEl = document.createElement('div');
      bodyEl.innerHTML = "Script is not ready, please refresh the page.";
      scriptCtn.appendChild(bodyEl);
    },

    updateScript : function(data) {
      var self = this;

      if ($("#scriptDivBody").length == 0 && data.job && data.job.configuration
          && data.job.configuration["pig.script"]) {
        var scriptContentEl = document.getElementById("scriptContent");
        var scriptTitleEl = document.getElementById('scriptDivTitle');
        var scriptNameEl = document.getElementById("scriptPopoverName");
        scriptNameEl.innerHTML = data.job.configuration["mapred.job.name"];

        var scriptBodyEl = document.createElement('div');
        scriptBodyEl.id = "scriptDivBody";
        scriptBodyEl.innerHTML = self.renderScript(data.job.configuration["pig.script"]);

        scriptContentEl.innerHTML = "";
        scriptContentEl.appendChild(scriptTitleEl);
        scriptContentEl.appendChild(scriptBodyEl);
      }
    },

    renderScript : function(script) {
      var lineCounter = 1;

      script = '<table><tr class="jobScript" id="scriptLine' + lineCounter + '">'
          + '<td class="lineNumber">' + lineCounter + '</td>'
          + '<td class="aliasM">&nbsp;</td>'
          + '<td class="aliasC">&nbsp;</td>'
          + '<td class="aliasR">&nbsp;</td><td>' + script;

      while (script.indexOf("<newLine>") != -1) {
        lineCounter++;
        script = script.replace('<newLine>', '</td></tr><tr class="jobScript" id="scriptLine'
            + lineCounter + '">'
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
     */
    highlineScript : function(colors, job, mouseAction, scrollTo) {
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
              var lineDiv = $("#scriptLine" + lineNum);
              if (mouseAction == "scriptClicked") {
                lineDiv.css("background-color", colors.scriptClicked);
                $("#scriptLine" + lineNum + " .lineNumber")
                .css("background-color", colors.scriptLineSelected);
              } else if (mouseAction == "scriptHovered") {
                lineDiv.css("background-color", colors.scriptHovered);
              } else {
                // On Cancel, unhighlight the lines.
                lineDiv.css("background-color", 'white');
                $("#scriptLine" + lineNum + " .lineNumber")
                .css("background-color", colors.scriptLineNum);
              }

              if (group === "C" || group === "M"  || group === "R") {
                self.setAliasType(group, mouseAction, lineNum);
              }
              if (lineNum < minLineNum) { minLineNum = lineNum; }
            }
          }
        }

        if (scrollTo && $('#scriptLine' + minLineNum).length > 0) {
          var scrollValue = $('#scriptLine' + minLineNum).offset().top
              - $('#scriptLine1').offset().top;
          $('#scriptDivBody').scrollTop(scrollValue);
        }
      }
    },

    /**
     * Toggle the alias type on/off for a line based on the mouse action.
     */
    setAliasType : function(aliasType, mouseAction, lineNum) {
      if (mouseAction == "scriptClicked") {
        $("#scriptLine" + lineNum + " .alias" + aliasType).html(aliasType);
      } else if (mouseAction == "scriptCancel") {
        $("#scriptLine" + lineNum + " .alias" + aliasType).html("&nbsp;");
      }
    },

    /**
     * Clear all the highlighting of the script.
     */
    clearScript : function(colors) {
      $(".jobScript").css("background-color", "white");
      $(".lineNumber").css("background-color", colors.scriptLineNum);
      $(".aliasM").html("&nbsp;");
      $(".aliasC").html("&nbsp;");
      $(".aliasR").html("&nbsp;");
    }
  };

  // Bind prototype to ctor
  ScriptView.fn.init.prototype = ScriptView.fn;
  return ScriptView;
});
