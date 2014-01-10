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

package com.twitter.ambrose.cascading;

import org.apache.hadoop.conf.Configuration;
import com.twitter.ambrose.service.impl.HRavenStatsWriteService;

/** Sends stats to ambrose service via HRaven.
  * @see com.twitter.ambrose.service.impl.HRavenStatsWriteService
  * @see com.twitter.ambrose.cascading.AmbroseCascadingNotifier
  * @see com.twitter.ambrose.cascading.AmbroseCascadingNotifierFactory
  */ 

public class HRavenAmbroseCascadingNotifier extends AmbroseCascadingNotifier {
    public HRavenAmbroseCascadingNotifier(Configuration jobConf) {
        super(HRavenStatsWriteService.forJob(jobConf));
    }
}
