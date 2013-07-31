package com.twitter.ambrose.hive.reporter;

/**
 * Factory class that takes care of creating a singleton instance of
 * {@link AmbroseHiveProgressReporter}. Hooks are retrieving the global
 * reporter instance from here through the lifecycle of the Hive script
 * 
 * @see EmbeddedAmbroseHiveProgressReporter
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 *
 */
public class AmbroseHiveReporterFactory {

  private AmbroseHiveReporterFactory() {}

  private static class EmbeddedReporterHolder {
    private static final EmbeddedAmbroseHiveProgressReporter INSTANCE = 
      new EmbeddedAmbroseHiveProgressReporter();
  }

  public static EmbeddedAmbroseHiveProgressReporter getEmbeddedProgressReporter() {
    return EmbeddedReporterHolder.INSTANCE;
  }

}
