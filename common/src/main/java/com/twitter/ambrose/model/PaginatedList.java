package com.twitter.ambrose.model;

import java.util.List;

/**
 * Holds a list of results, as well as pagination info.
 */
public class PaginatedList<T> {
  private List<T> results;
  private String nextPageStart;

  public PaginatedList(List<T> results) {
    this.results = results;
  }

  public List<T> getResults() {
    return results;
  }

  public String getNextPageStart() {
    return nextPageStart;
  }

  public void setNextPageStart(String startKey) {
    this.nextPageStart = startKey;
  }
}
