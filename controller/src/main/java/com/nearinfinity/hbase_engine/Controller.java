package com.nearinfinity.hbase_engine;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

public class Controller {

  public static EngineHandler handler;

  public static Engine.Processor processor;

  public static void main(String [] args) throws TException {
    try {
      handler = new EngineHandler();
      processor = new Engine.Processor(handler);

      Runnable simple = new Runnable() {
        public void run() {
          simple(processor);
        }
      };

      new Thread(simple).start();
    } catch (Exception x) {
      x.printStackTrace();
    }

      TTransport transport;
      transport = new TSocket("localhost", 8080);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);

      Engine.Client client = new Engine.Client(protocol);
      client.open();
      transport.close();
  }

  public static void simple(Engine.Processor processor) {
    try {
      TServerTransport serverTransport = new TServerSocket(8080);
      TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

      // Use this for a multithreaded server
      // TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

      System.out.println("Starting the hbase_engine controller...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
