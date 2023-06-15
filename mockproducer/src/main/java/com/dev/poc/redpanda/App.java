package com.dev.poc.redpanda;

import com.dev.poc.redpanda.Producer;

/**
 * Hello world!
 *
 */
public class App {
  public static void main(String[] args) {
    Producer producer = new Producer();
    try {
      producer.runAlways("nums-raw");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
