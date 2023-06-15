package com.dev.poc.flink.dto;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NumEvent {
  private long timestamp;
  private int number;
  private int cube;

  public void setTimestamp(long timestamp){
    this.timestamp = timestamp;
  }
  public long getTimestamp(){
    return this.timestamp;
  }

  public void setNumber(int num){
    this.number = num;
  }
  public int getNumber(){
    return this.number;
  }

  public void setCube(int num){
    this.cube = num;
  }
  public int getCube(){
    return this.cube;
  }
}