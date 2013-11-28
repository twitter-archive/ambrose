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
define(['lib/jquery'], function($) {
  var scriptSelected = "#FFFF80"; // Yellow
  var scriptClicked = "#FFD880";
  var scriptClickedMark = "#AAAAAA";
  var scriptUntouched = "#F5F5F5";

  return {
    /**
     * Create the div used for script view.
     */
    createScriptDiv : function(script, jobName) {
      var self = this;
      $('#scriptDiv').draggable({
        handle: '#scriptDivTitle'
      }).resizable();

      var scriptCtn = document.getElementById('scriptContent');
      var titleEl = document.createElement('div');
      titleEl.className = "modal-header";
      titleEl.id = "scriptDivTitle";
      if (!jobName) { jobName = "Pig Script" }
      titleEl.innerHTML = '<span id="scriptPopoverName">' + jobName + '</span>';

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
        self.clearScript();
        if ($("#scriptDivBody").length > 0) {
          $('#scriptDivBody').scrollTop(0);
        }
      };

      titleEl.appendChild(scriptCloseBtn);
      titleEl.appendChild(scriptRefreshBtn);
      scriptCtn.appendChild(titleEl);

      var bodyEl = document.createElement('div');
      if (script == null) {
        script = "Script is not ready, please refresh the page.";
      } else {
        bodyEl.id = "scriptDivBody";
        script = self.renderScript(script);
      }
      bodyEl.innerHTML = script;
      scriptCtn.appendChild(bodyEl);
    },

    updateScript : function(event) {
      var self = this;

      if ($("#scriptDivBody").length == 0 && event.payload.job && event.payload.job.configuration
          && event.payload.job.configuration["pig.script"]) {
          var scriptContentEl = document.getElementById("scriptContent");
          var scriptTitleEl = document.getElementById('scriptDivTitle');
          var scriptNameEl = document.getElementById("scriptPopoverName");
          scriptNameEl.innerHTML = event.payload.job.configuration["mapred.job.name"];

          var scriptBodyEl = document.createElement('div');
          scriptBodyEl.id = "scriptDivBody";
          scriptBodyEl.innerHTML = self.renderScript(event.payload.job.configuration["pig.script"]);

          scriptContentEl.innerHTML = "";
          scriptContentEl.appendChild(scriptTitleEl);
          scriptContentEl.appendChild(scriptBodyEl);
      }
    },

    renderScript : function(script) {
      var lineCounter = 1;
      //debugger
      script = '<table><tr class="jobScript" id="scriptLine' + lineCounter + '">'
          + '<td class="lineNumber">' + lineCounter + '</td>'
          + '<td class="aliasM">&nbsp;</td>'
          + '<td class="aliasC">&nbsp;</td>'
          + '<td class="aliasR">&nbsp;</td><td>' + script;

      while (script.indexOf("<newLine>") != -1) {
        lineCounter++;
        script = script.replace('<newLine>', '</td></tr><tr class="jobScript" id="scriptLine' + lineCounter + '">'
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
    highlineScript : function(job, mouseAction) {
      if (!job) return ; // null check

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
                lineDiv.css("background-color", scriptClicked);
                $("#scriptLine" + lineNum + " .lineNumber")
                .css("background-color", scriptClickedMark);
              } else if (mouseAction == "scriptHovered") {
                lineDiv.css("background-color", scriptSelected);
              } else {
                lineDiv.css("background-color", 'white');
                $("#scriptLine" + lineNum + " .lineNumber")
                .css("background-color", scriptUntouched);
              }

              if (group === "C") {
                if (mouseAction == "scriptClicked") {
                  $("#scriptLine" + lineNum + " .aliasC").html("C");
                } else if (mouseAction == "scriptCancel") {
                  $("#scriptLine" + lineNum + " .aliasC").html("&nbsp;");
                }
              } else if (group === "M") {
                if (mouseAction == "scriptClicked") {
                  $("#scriptLine" + lineNum + " .aliasM").html("M");
                } else if (mouseAction == "scriptCancel") {
                  $("#scriptLine" + lineNum + " .aliasM").html("&nbsp;");
                }
              } else if (group === "R") {
                if (mouseAction == "scriptClicked") {
                  $("#scriptLine" + lineNum + " .aliasR").html("R");
                } else if (mouseAction == "scriptCancel") {
                  $("#scriptLine" + lineNum + " .aliasR").html("&nbsp;");
                }
              }
              if (lineNum < minLineNum) { minLineNum = lineNum; }
            }
          }
        }

        if ($('#scriptLine' + minLineNum).length > 0) {
          var scrollValue =
            $('#scriptLine' + minLineNum).offset().top - $('#scriptLine1').offset().top;
          $('#scriptDivBody').scrollTop(scrollValue);
        }
      }
    },

    /**
     * Clear all the highlighting of the script.
     */
    clearScript : function() {
      $(".jobScript").css("background-color", "white");
      $(".lineNumber").css("background-color", "white");
      $(".aliasM").html("&nbsp;");
      $(".aliasC").html("&nbsp;");
      $(".aliasR").html("&nbsp;");
    }
  };
});
