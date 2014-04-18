package com.twitter.ambrose.util;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.hadoop.MapReduceUtils;
import com.twitter.ambrose.service.StatsWriteService;

public final class AmbroseUtils {

  private AmbroseUtils() {}
  
  private static Log log = LogFactory.getLog(MapReduceUtils.class);
  
  /**
   * Handle and ignore any IOException while sending DagNodeNameMap to statsWriteService
   * @param statsWriteService to send DagNodeNameMap to
   * @param scriptId unique id of the script
   * @param dagNodeNameMap map of name to dag node
   */
  public static void sendDagNodeNameMap(StatsWriteService statsWriteService, String scriptId, Map dagNodeNameMap) {
    try {
      statsWriteService.sendDagNodeNameMap(scriptId, dagNodeNameMap);
    } catch (IOException e) {
      log.error("Couldn't send dag to StatsWriteService", e);
    }
  }

  /**
   * Handle and ignore any IOException while sending Event to statsWriteService
   * @param statsWriteService to send event to
   * @param scriptId unique id of the script
   * @param event ambrose event
   */
  public static void pushEvent(StatsWriteService statsWriteService, String scriptId, Event event) {
    try {
      statsWriteService.pushEvent(scriptId, event);
    } catch (IOException e) {
      log.error("Couldn't send event to StatsWriteService", e);
    }
  }
  
  /**
   * Send workflow progress event
   * @param statsWriteService
   * @param scriptId
   * @param progress
   */
  public static void pushWorkflowProgressEvent(StatsWriteService statsWriteService, String scriptId, int progress) {
    Map<Event.WorkflowProgressField, String> eventData = Maps.newHashMap();
    eventData.put(Event.WorkflowProgressField.workflowProgress, Integer.toString(progress));
    pushEvent(statsWriteService, scriptId, new Event.WorkflowProgressEvent(eventData));
  }

}
