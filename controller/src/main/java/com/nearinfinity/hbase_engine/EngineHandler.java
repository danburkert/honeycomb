package com.nearinfinity.hbase_engine;

public class EngineHandler implements Engine.Iface {

  public EngineHandler() {}

  public void open() {
    System.out.println("Handler Opened!");
  }
}
