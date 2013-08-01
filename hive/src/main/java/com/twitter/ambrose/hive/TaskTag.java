/*
Copyright 2013, Lorand Bendig

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
package com.twitter.ambrose.hive;

import java.util.HashMap;
import java.util.Map;

/**
 * Additional job properties
 * 
 * @see org.apache.hadoop.hive.ql.exec.Task
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
public enum TaskTag {

  COMMON_JOIN(1),
  CONVERTED_MAPJOIN(2),
  CONVERTED_LOCAL_MAPJOIN(3),
  BACKUP_COMMON_JOIN(4),
  LOCAL_MAPJOIN(5),
  MAPJOIN_ONLY_NOBACKUP(6);

  private static final Map<Integer, String> lookup = new HashMap<Integer, String>();

  static {
    for (TaskTag s : TaskTag.values()) {
      lookup.put(s.getId(), s.toString());
    }
  }

  private int id;

  private TaskTag(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public static String get(int code) {
    return lookup.get(code);
  }

}
