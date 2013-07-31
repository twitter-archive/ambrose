package com.twitter.ambrose.hive.reporter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.twitter.ambrose.hive.AmbroseHiveUtil;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.server.ScriptStatusServer;
import com.twitter.ambrose.service.impl.InMemoryStatsService;

/**
 * Subclass of {@link AmbroseHiveProgressReporter} that starts a ScriptStatusServer embedded in
 * the running Hive client VM. Stats are collected using by this class via InMemoryStatsService,
 * which is what serves stats to ScriptStatusServer.
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public class EmbeddedAmbroseHiveProgressReporter extends AmbroseHiveProgressReporter {

  private static final Log LOG = LogFactory.getLog(EmbeddedAmbroseHiveProgressReporter.class);

  private InMemoryStatsService service;
  private ScriptStatusServer server;

  /**
   * internal eventMap field unfolded from InMemoryStatsService
   * Needed to be able to save events of workflows and restore all of the
   * when script is replayed at the end
   */
  private SortedMap<Integer, Event<?>> _eventMap;

  EmbeddedAmbroseHiveProgressReporter() {
    super(new InMemoryStatsService());
    this.service = (InMemoryStatsService) getStatsWriteService();
    this.server = new ScriptStatusServer(service, service);
    this.server.start();
    initInternal();
  }
  
  @SuppressWarnings("unchecked")
  private void initInternal() {
    try {
      Field eventMapField = AmbroseHiveUtil
          .getInternalField(InMemoryStatsService.class, "eventMap");
      _eventMap = (SortedMap<Integer, Event<?>>) eventMapField.get(service);
    }
    catch (Exception e) {
      LOG.fatal("Can't access to eventMap/dagNodeNameMap fields at "
          + InMemoryStatsService.class.getName() + "!");
      throw new RuntimeException("Incompatible Hive API found!", e);
    }
  }
  
  /**
   * Saves events and DAGNodes for a given workflow
   */
  @Override
  public void saveEventStack() {
    allEvents.putAll(_eventMap);
    allDagNodes.putAll(service.getDagNodeNameMap(null));
  }

  /**
   * Restores events and DAGNodes of all workflows within a script This enables
   * to replay all the workflows when the script finishes
   */
  @Override
  public void restoreEventStack() {
    _eventMap.putAll(allEvents);
    service.getDagNodeNameMap(null).putAll(allDagNodes);
  }
  
  public void stopServer() {
    LOG.info("Stopping Ambrose Server...");
    server.stop();
  }
  
  public void flushJsonToDisk() {
    try {
      service.flushJsonToDisk();
    }
    catch (IOException e) {
      LOG.warn("Couldn't write json to disk", e);
    }
  }

  @Override
  public void resetAdditionals() {
    _eventMap.clear();
  }

}
