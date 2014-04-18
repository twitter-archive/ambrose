package com.twitter.ambrose.pig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class that represents information about a data output of a job.
 */
public class OutputInfo {
  private String name;
  private String location;
  private long numberBytes;
  private long numberRecords;
  private boolean successful;
  private String functionName;
  private String alias;

  @JsonCreator
  public OutputInfo(@JsonProperty("name") String name,
                    @JsonProperty("location") String location,
                    @JsonProperty("numberBytes") long numberBytes,
                    @JsonProperty("numberRecords") long numberRecords,
                    @JsonProperty("successful") boolean successful,
                    @JsonProperty("functionName") String functionName,
                    @JsonProperty("alias") String alias) {
    this.name = name;
    this.location = location;
    this.numberBytes = numberBytes;
    this.numberRecords = numberRecords;
    this.successful = successful;
    this.functionName = functionName;
    this.alias = alias;
  }

  public String getName() { return name; }
  public String getLocation() { return location; }
  public long getNumberBytes() { return numberBytes; }
  public long getNumberRecords() { return numberRecords; }
  public boolean isSuccessful() { return successful; }
  public String getFunctionName() { return functionName; }
  public String getAlias() { return alias; }
}
