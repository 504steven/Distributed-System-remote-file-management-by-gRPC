package com.ggsddu.fileservice.datanode;

import com.ggsddu.fileservice.leadernode.LeaderForwardFile;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

public class FileServiceServer {
  private static final Logger logger = Logger.getLogger(FileServiceServer.class.getName());

  public static final String DATA_NODE_IP = "192.168.0.162"; //"localhost";
  public static final int DATA_NODE_PORT = 60051;   //datanode的端口号，全部都是60051

  private static final String LEADER_NODE_IP = "192.168.0.10";  // "localhost";
  private static final int LEADER_NODE_PORT = 50051;   //leadernode的端口号

  private Server mServer;

  private void start() throws IOException {
    /* The port on which the mServer should run com.ggsddu.fileservice.LeaderFileServiceServer*/

    mServer = ServerBuilder.forPort(DATA_NODE_PORT)
            .addService(new FileServiceImpl())
            .build()
            .start();
    logger.info("Server started, listening on " + DATA_NODE_PORT);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC mServer since JVM is shutting down");
        FileServiceServer.this.stop();
        System.err.println("*** mServer shut down");
      }
    });
  }

  private void stop() {
    if (mServer != null) {
      mServer.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (mServer != null) {
      mServer.awaitTermination();
    }
  }

  /**
   * Main launches the mServer from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {

    //链接leader node, 传输本data node 的信息
//    NodeServiceClient client = new NodeServiceClient(LEADER_NODE_IP, LEADER_NODE_PORT);
//
//    try {
//      client.transferNodeInfo();
//    } finally {
//      try {
//        Thread.sleep(500);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//      client.shutdown();
//    }


    final FileServiceServer server = new FileServiceServer();
    server.start();
    server.blockUntilShutdown();
  }
}
