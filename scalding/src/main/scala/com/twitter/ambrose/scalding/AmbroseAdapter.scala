/*
Copyright 2013 Twitter, Inc.

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
package com.twitter.ambrose.scalding

import cascading.flow.Flow
import com.twitter.ambrose.cascading.AmbroseCascadingNotifier
import com.twitter.scalding.Job
import org.apache.hadoop.mapreduce.JobContext


/** To enable embedded ambrose server in your scalding job, just add this trait to it.
 *  Then, while the job is running, open http://localhost:8080 with your browser.
 *  @see com.twitter.ambrose.cascading.EmbeddedAmbroseCascadingNotifier for details on
 * configuring the port etc.
 */
trait AmbroseAdapter extends Job {

  @transient val ambroseListener: Option[AmbroseCascadingNotifier] = None

  override def buildFlow: Flow[_] = {
    val flow = super.buildFlow
    ambroseListener match {
      case Some(listener) =>
        flow.addListener(listener)
        flow.addStepListener(listener)
      case _ => // do nothing
    }
    flow
  }
}
