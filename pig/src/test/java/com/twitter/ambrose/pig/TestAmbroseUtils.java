package com.twitter.ambrose.pig;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.junit.Test;

public class TestAmbroseUtils {

  @Test
  public void testNewPPNLWithoutException() {
    PigProgressNotificationListener ppnl = 
        AmbroseUtil.newPPNLWithoutExceptions(new ExceptionThrowingPPNL());
    ppnl.initialPlanNotification(null, null);
    ppnl.jobFailedNotification(null, null);
  }

  private static class ExceptionThrowingPPNL implements PigProgressNotificationListener {
    @Override
    public void initialPlanNotification(String arg0, MROperPlan arg1) {
      throw new UnsupportedOperationException("NYI");

    }

    @Override
    public void jobFailedNotification(String arg0, JobStats arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void jobFinishedNotification(String arg0, JobStats arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void jobStartedNotification(String arg0, String arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void jobsSubmittedNotification(String arg0, int arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void launchCompletedNotification(String arg0, int arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void launchStartedNotification(String arg0, int arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void outputCompletedNotification(String arg0, OutputStats arg1) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void progressUpdatedNotification(String arg0, int arg1) {
      throw new UnsupportedOperationException("NYI");
    }		
  }
}
