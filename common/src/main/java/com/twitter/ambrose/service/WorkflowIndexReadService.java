package com.twitter.ambrose.service;

import java.io.IOException;
import java.util.Map;

import com.twitter.ambrose.model.PaginatedList;
import com.twitter.ambrose.model.WorkflowSummary;

/**
 * Interface to fetch paginated lists of WorkflowSummaries.
 */
public interface WorkflowIndexReadService {
  /**
   * Returns map of cluster id to name.
   *
   * @return map of cluster id to name.
   * @throws IOException
   */
  Map<String, String> getClusters() throws IOException;

  /**
   * Returns workflow summaries for a given status and optional userId filter.
   *
   * @param cluster cluser to return results for.
   * @param status workflow status.
   * @param userId user to filter on, or null if all users requested.
   * @param numResults how many results to return.
   * @param startKey start key for the page of results to return.
   * @return paginated list of workflow summaries.
   */
  PaginatedList<WorkflowSummary> getWorkflows(String cluster, WorkflowSummary.Status status,
      String userId, int numResults, byte[] startKey) throws IOException;
}
