package com.nearinfinity.hbase_engine;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

public class ThriftEngineDriver {
    public static ThriftEngine engine;

    public static Engine.Processor processor;

    public static void main(String [] args) throws TException {
        try {
            String serverName = "hbase.master";
            String hostWithPort = "localhost:53268";
            // This has to be declared as final (can't be passed in) because it's accessed by an inner class...dammit Java - ABC
            final int serverPort = 8080;

            if (args.length >= 3)
            {
                serverName = args[1];
                hostWithPort = args[2];
            }
            else
            {
                System.out.println("Usage: ThriftEngineDriver.java <server name> <host:hbase_port>");
                System.out.println("Using default server name " + serverName);
                System.out.println("Using default HBase host:port " + hostWithPort);
            }

            engine = new ThriftEngine(serverName, hostWithPort);
            processor = new Engine.Processor(engine);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor, serverPort);
                }
            };

            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(Engine.Processor processor, int port) {
        TServerTransport serverTransport = null;
        try {
            serverTransport = new TServerSocket(port);
        } catch (TTransportException e) {
            e.printStackTrace();
            System.exit(1);
        }
        TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));

        try {
            // Use this for a multithreaded server
            // TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the Thrift engine controller...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }

        server.stop();
    }
}
