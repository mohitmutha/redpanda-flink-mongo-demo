package com.dev.poc.flink.dto;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NumEvent {
  private long timestamp;
  private int number;

  public void setTs(long timestamp){
    this.timestamp = timestamp;
  }
  public long getTs(){
    return this.timestamp;
  }

  public void setNumber(int num){
    this.number = num;
  }
  public int getNumber(){
    return this.number;
  }
}