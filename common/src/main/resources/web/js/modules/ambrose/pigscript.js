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
  return {
    testing : function() {
      alert('testing');
    },

    /**
     * Create the div used for script view.
     */
    createScriptDiv : function(script) {
      var self = this;
      $('#scriptDiv').draggable({
        handle: '#scriptDivTitle'
      }).resizable();

      var scriptCtn = document.getElementById('scriptContent');
      var titleEl = document.createElement('div');
      titleEl.className = "modal-header";
      titleEl.id = "scriptDivTitle";
      titleEl.innerHTML = "Pig Script";

      var scriptCloseBtn = document.createElement('button');
      scriptCloseBtn.className = "close";
      scriptCloseBtn.innerHTML = "X";
      scriptCloseBtn.onclick = function() {
        $("#scriptDiv").toggleClass('hidden', true);
      };

      var scriptRefreshBtn = document.createElement('button');
      scriptRefreshBtn.className = "close";
      scriptRefreshBtn.innerHTML = "&#8635;&nbsp;&nbsp;";
      scriptRefreshBtn.onclick = function() {
        $(".jobScript").css("background-color", "white");
        if ($("#scriptDivBody").length > 0) {
          $('#scriptDivBody').animate({scrollTop: 0}, 500);
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
      script = '<div class="jobScript" id="scriptLine' + lineCounter + '">'
          + '<span class="lineNumber">' + lineCounter + "</span>" + script;

      while (script.indexOf("<newLine>") != -1) {
        lineCounter++;
        script = script.replace('<newLine>', '</div><div class="jobScript" id="scriptLine'
            + lineCounter + '">' + '<span class="lineNumber">' + lineCounter + '</span>');
      }

      return script + "</div>";
    },

    /**
     * Handles the mouse interaction, and highlight the part of the script that corresponds to
     * the alias of the node hovered over.
     */
    highlineScript : function(colors, node) {
      $(".jobScript").css("background-color", "white");

      if (node.data && node.data.configuration && node.data.configuration["pig.alias.location"]) {
        // Aliases are in order of M:...C:... R:...
        var aliases = node.data.configuration["pig.alias.location"].split(/(?=[A-Z]:)/);
        var minLineNum = 10000; // Should be a large enough initial value.

        for (var i = 0; i < aliases.length; i++) {
          var group = aliases[i].substring(0, 1);
          var lines = aliases[i].match(/\[(.*?)\,/g);
          if (lines != null) {
            for (var j = 0; j < lines.length; j++) {
              var lineNum = Number(lines[j].substring(1, lines[j].length - 1));
              // Highlights Mapper, Reducer and Combiner using different color.
              if (group === "M") {
                $("#scriptLine" + lineNum).css("background-color", colors.scriptHighlightMap);
              } else if (group === "R") {
                $("#scriptLine" + lineNum).css("background-color", colors.scriptHighlightReduce);
              } else if (group === "C") {
                $("#scriptLine" + lineNum).css("background-color", colors.scriptHighlightCombine);
              }
              if (lineNum < minLineNum) { minLineNum = lineNum; }
            }
          }
        }

        if ($('#scriptLine' + minLineNum).length > 0) {
          var scrollValue =
            $('#scriptLine' + minLineNum).offset().top - $('#scriptLine1').offset().top;
          $('#scriptDivBody').animate({scrollTop: scrollValue}, 500);
        }
      }
    }
  };
});
