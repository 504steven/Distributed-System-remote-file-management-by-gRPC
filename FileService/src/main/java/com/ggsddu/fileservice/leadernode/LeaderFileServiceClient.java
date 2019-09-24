package com.ggsddu.fileservice.leadernode;

import com.ggsddu.fileservice.MongoDBOperate;
import com.ggsddu.fileservice.datamodels.Server;
import com.google.protobuf.ByteString;
import fileservice.FileService;
import fileservice.FileserviceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.ggsddu.fileservice.Delimiter.DELIMITER;

public class LeaderFileServiceClient {
  private static final Logger logger = Logger.getLogger(LeaderFileServiceClient.class.getName());

  private static final String SUPER_NODE_IP = "127.0.0.1";
  private static final int SUPER_NODE_PORT = 50051;

//  private static final String SUPER_NODE_IP = "192.168.0.9";
//  private static final int SUPER_NODE_PORT = 9000;

  private static final String USERNAME = "ggsddu";

  private final ManagedChannel mChannel;
  private final FileserviceGrpc.FileserviceBlockingStub mBlockingStub;
  private final FileserviceGrpc.FileserviceStub mAsyncStub;

  public LeaderFileServiceClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
            // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            // needing certificates.
            .usePlaintext()
            .build());

  }

  LeaderFileServiceClient(ManagedChannel channel) {
    this.mChannel = channel;
    mBlockingStub = FileserviceGrpc.newBlockingStub(channel);
    mAsyncStub = FileserviceGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
    @SuppressWarnings("Duplicates")
    public void fileDelete(String userName, String fileName){
        StreamObserver<FileService.ack> responseObserver = new StreamObserver<FileService.ack>() {
            @Override
            public void onNext(FileService.ack value) {
                if(value.getSuccess()){
                    System.out.println("delete file");
                }else{
                    System.out.println("not delete  file");
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };

        FileService.FileInfo fileInfo = FileService.FileInfo.newBuilder().setUsername(userName).setFilename(fileName).build();
        mAsyncStub.fileDelete(fileInfo, responseObserver);
    }

  public void fileSearch(String userName, String fileName){
      StreamObserver<FileService.ack> responseObserver = new StreamObserver<FileService.ack>() {
          @Override
          public void onNext(FileService.ack value) {
              if(value.getSuccess()){
                  System.out.println("has file");
              }else{
                  System.out.println("No file");
              }
          }

          @Override
          public void onError(Throwable t) {

          }

          @Override
          public void onCompleted() {

          }
      };

      FileService.FileInfo fileInfo = FileService.FileInfo.newBuilder().setUsername(userName).setFilename(fileName).build();
      mAsyncStub.fileSearch(fileInfo, responseObserver);
  }

  public void fileList(String username){
    StreamObserver<FileService.FileListResponse> responseObserver = new StreamObserver<FileService.FileListResponse>() {
      @Override
      public void onNext(FileService.FileListResponse value) {
        String filenames = value.getFilenames();
        System.out.println(filenames);
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    };

    FileService.UserInfo userInfo = FileService.UserInfo.newBuilder().setUsername(username).build();
    mAsyncStub.fileList(userInfo, responseObserver);
  }

  @SuppressWarnings("Duplicates")
  public void downloadFile(final String filename){
      StreamObserver<FileService.FileData> responseObserver = new StreamObserver<FileService.FileData>() {
        private String mMessage = "";
        private BufferedOutputStream cBufferedOutputStream = null;
        private final String CLIENT_FILE_DIRECTORY = "."+DELIMITER+"client_files"+DELIMITER;

        @Override
        public void onNext(FileService.FileData value) {
          // Print count

          byte[] data = value.getData().toByteArray();
          String username = value.getUsername();
          String filename = value.getFilename();

          // Create directory if not exist
          File dir = new File(CLIENT_FILE_DIRECTORY);
          dir.mkdir();
          dir = new File(CLIENT_FILE_DIRECTORY + username);
          dir.mkdir();

          try {
            System.out.println("begin get info");
            if (cBufferedOutputStream == null) {
              cBufferedOutputStream = new BufferedOutputStream(new FileOutputStream(CLIENT_FILE_DIRECTORY + username + DELIMITER + filename));
            }
            cBufferedOutputStream.write(data);
            cBufferedOutputStream.flush();
          } catch (Exception e) {
//            e.printStackTrace();
          }
        }

        @Override
        public void onError(Throwable t) {
            System.out.println("No such file");
        }

        @Override
        public void onCompleted() {
            try {
              cBufferedOutputStream.close();
            } catch (IOException e) {
//              e.printStackTrace();
            } finally {
              cBufferedOutputStream = null;
            }
          }
      };

      FileService.FileInfo fileInfo = FileService.FileInfo.newBuilder().setUsername(USERNAME).setFilename(filename).build();
      mAsyncStub.downloadFile(fileInfo, responseObserver);
  }

  @SuppressWarnings("Duplicates")
  public void uploadFile(final String filepath) {
    logger.info("tid: " +  Thread.currentThread().getId() + ", Will try to getBlob");
    StreamObserver<FileService.ack> responseObserver = new StreamObserver<FileService.ack>() {

      @Override
      public void onNext(FileService.ack value) {
        logger.info("Client response onNext");
        logger.info(value.getMessage());
      }

      @Override
      public void onError(Throwable t) {
        logger.info("Client response onError");
        t.printStackTrace();
      }

      @Override
      public void onCompleted() {
        logger.info("Client response onCompleted");
      }
    };

    StreamObserver<FileService.FileData> requestObserver = mAsyncStub.uploadFile(responseObserver);
    try {

      File file = new File(filepath);
      if (file.exists() == false) {
        logger.info("File does not exist");
        return;
      }
      try {
        BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(file));
        int bufferSize = 512 * 1024; // 512k
        byte[] buffer = new byte[bufferSize];
        int tmp = 0;
        int size = 0;
        while ((tmp = bInputStream.read(buffer)) > 0) {
          size += tmp;
          ByteString byteString = ByteString.copyFrom(buffer);
          FileService.FileData req = FileService.FileData.newBuilder().setUsername(USERNAME).setFilename(file.getName()).setData(byteString).build();
          requestObserver.onNext(req);
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (RuntimeException e) {
      requestObserver.onError(e);
      throw e;
    }
    requestObserver.onCompleted();
  }

  public void syncCall() {
      mBlockingStub.fileDelete(FileService.FileInfo.newBuilder().build());
  }

  public static void main(String[] args) throws Exception {
    LeaderFileServiceClient client = new LeaderFileServiceClient(SUPER_NODE_IP, SUPER_NODE_PORT);

    // Upload Test
//    try {
//      client.uploadFile("/Users/will/Downloads/test/test101.m4v");
//      logger.info("Done with upload");
//    } finally {
//      try {
//        Thread.sleep(500);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//      client.shutdown();
//    }

    // Download Test
//    try {
//      client.downloadFile("test101.m4v");
//      logger.info("Done with download");
//    } finally {
//      try {
//        Thread.sleep(500);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//      client.shutdown();
//    }

      // File Search Test
//      try {
//          client.fileSearch(USERNAME, "test100.mov");
//          logger.info("Done with file search");
//      } finally {
//          try {
//              Thread.sleep(500);
//          } catch (InterruptedException e) {
//              e.printStackTrace();
//          }
//          client.shutdown();
//      }

      // File List Test
//      try {
//          client.fileList(USERNAME);
//          logger.info("Done with filelist");
//      } finally {
//          try {
//              Thread.sleep(500);
//          } catch (InterruptedException e) {
//              e.printStackTrace();
//          }
//          client.shutdown();
//      }

      // File Delete Test
//      try {
//          client.fileDelete(USERNAME, "test102.mov");
//          logger.info("Done with delete");
//      } finally {
//          try {
//              Thread.sleep(500);
//          } catch (InterruptedException e) {
//              e.printStackTrace();
//          }
//          client.shutdown();
//      }
  }
}
