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
