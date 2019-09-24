package com.ggsddu.fileservice.leadernode;

import com.ggsddu.fileservice.Delimiter;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

public class LeaderFileServiceServer {
  private static final Logger logger = Logger.getLogger(LeaderFileServiceServer.class.getName());

//  private static final String LEADER_NODE_IP = "192.168.0.10";
  private static final String LEADER_NODE_IP = "127.0.0.1";
  private static final int LEADER_NODE_PORT = 50051;

 private static final String SUPER_NODE_IP = "192.168.0.9";
//  private static final String SUPER_NODE_IP = "127.0.0.1";


  private static final int SUPER_NODE_PORT = 9000;

  private Server mServer;

  private void start() throws IOException {
    /* The port on which the mServer should run com.ggsddu.fileservice.LeaderFileServiceServer*/

    mServer = ServerBuilder.forPort(LEADER_NODE_PORT)
            .addService(new LeaderFileServiceImpl())
            .build()
            .start();
    logger.info("Server started, listening on " + LEADER_NODE_PORT);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC mServer since JVM is shutting down");
        LeaderFileServiceServer.this.stop();
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
    new Delimiter();
    final LeaderFileServiceServer server = new LeaderFileServiceServer();
    server.start();

    LeaderForwardFile client = new LeaderForwardFile(SUPER_NODE_IP, SUPER_NODE_PORT);
    client.syncCall();
    for(int i = 0; i < 100 ; i++) {
      client.uploadFile("E:\\__BackUp\\__Ppriva\\CollegeUniversity_Material\\SJSU_Software\\__275_Gash\\CMPE275\\project_1\\fluffyedges\\FileService\\node_files\\user1\\wo0.jpg","ggsddu"+i);
    }

//    Thread thread = new Thread(){
//      public void run(){
//        System.out.println("Thread for super node communicating running");
//        while (true) {
//          try {
//            LeaderForwardFile client = new LeaderForwardFile(SUPER_NODE_IP, SUPER_NODE_PORT);
//            client.postLeaderInfo(LEADER_NODE_IP, LEADER_NODE_PORT, "GGsDDu-cluster");
//          } catch (Exception e) {
//            e.printStackTrace();
//          } finally {
//            try {
//              Thread.sleep(5000);
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            }
//          }
//        }
//      }
//    };
//    thread.start();

    server.blockUntilShutdown();
  }
}