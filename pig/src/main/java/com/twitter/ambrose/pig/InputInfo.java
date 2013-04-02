package com.twitter.ambrose.pig;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Class that represents information about a data input to a job.
 */
@JsonSerialize(
  include=JsonSerialize.Inclusion.NON_NULL
)
public class InputInfo {
  private String name;
  private String location;
  private long numberBytes;
  private long numberRecords;
  private boolean successful;
  private String inputType;

  @JsonCreator
  public InputInfo(@JsonProperty("name") String name,
                   @JsonProperty("location") String location,
                   @JsonProperty("numberBytes") long numberBytes,
                   @JsonProperty("numberRecords") long numberRecords,
                   @JsonProperty("successful") boolean successful,
                   @JsonProperty("inputType") String inputType) {
    this.name = name;
    this.location = location;
    this.numberBytes = numberBytes;
    this.numberRecords = numberRecords;
    this.successful = successful;
    this.inputType = inputType;
  }

  public String getName() { return name; }
  public String getLocation() { return location; }
  public long getNumberBytes() { return numberBytes; }
  public long getNumberRecords() { return numberRecords; }
  public boolean isSuccessful() { return successful; }
  public String getInputType() { return inputType; }
}
