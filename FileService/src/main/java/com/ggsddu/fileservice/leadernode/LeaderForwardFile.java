package com.ggsddu.fileservice.leadernode;

import com.google.protobuf.ByteString;
import fileservice.FileService;
import fileservice.FileserviceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.ggsddu.fileservice.Delimiter.DELIMITER;

public class LeaderForwardFile {
    private static final Logger logger = Logger.getLogger(LeaderForwardFile.class.getName());

    private final ManagedChannel mChannel;
    private final FileserviceGrpc.FileserviceBlockingStub mBlockingStub;
    private final FileserviceGrpc.FileserviceStub mAsyncStub;

    private final LeaderForwardFile self = this;


    public LeaderForwardFile(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid needing certificates.
                .usePlaintext()
                .build());
    }

    public LeaderForwardFile(ManagedChannel channel) {
        this.mChannel = channel;
        mBlockingStub = FileserviceGrpc.newBlockingStub(channel);
        mAsyncStub = FileserviceGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @SuppressWarnings("Duplicates")
    public void downloadFile(final String filename, final String username, final StreamObserver<FileService.FileData> responseObserver1){
        StreamObserver<FileService.FileData> responseObserver = new StreamObserver<FileService.FileData>() {
            private String mMessage = "";
            private BufferedOutputStream cBufferedOutputStream = null;
            private final  String SERVER_FILE_DIRECTORY = "." + DELIMITER + "server_files" + DELIMITER;
            //////////////////////

            @Override
            public void onNext(FileService.FileData value) {
                // Print count
                responseObserver1.onNext(value);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
                responseObserver1.onCompleted();
            }
        };
        /////////////

//            @Override
//            public void onNext(FileService.FileData value) {
//                // Print count
//
//                byte[] data = value.getData().toByteArray();
//                String username = value.getUsername();
//                String filename = value.getFilename();
//
//                // Create directory if not exist
//                File dir = new File(SERVER_FILE_DIRECTORY);
//                dir.mkdir();
//                dir = new File(SERVER_FILE_DIRECTORY + username);
//                dir.mkdir();
//
//                try {
//                    System.out.println("begin get info");
//                    if (cBufferedOutputStream == null) {
//                        cBufferedOutputStream = new BufferedOutputStream(new FileOutputStream(SERVER_FILE_DIRECTORY + username + DELIMITER + filename));
//                    }
//                    cBufferedOutputStream.write(data);
//                    cBufferedOutputStream.flush();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//            @Override
//            public void onError(Throwable t) {
//            }
//
//            @Override
//            public void onCompleted() {
//                try {
//                    cBufferedOutputStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } finally {
//                    cBufferedOutputStream = null;
//                }
//
//                try {
//                    BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(SERVER_FILE_DIRECTORY + username + DELIMITER + filename));
//                    int bufferSize = 512 * 1024; // 512k
//                    byte[] buffer = new byte[bufferSize];
//                    int tmp = 0;
//                    int size = 0;
//                    while ((tmp = bInputStream.read(buffer)) > 0) {
//                        size += tmp;
//                        ByteString byteString = ByteString.copyFrom(buffer);
//                        FileService.FileData req = FileService.FileData.newBuilder().setUsername(username).setFilename(filename).setData(byteString).build();
//                        responseObserver1.onNext(req);
//                    }
//                    responseObserver1.onCompleted();
//                    bInputStream.close();
//
//                    try {
//                        File file = new File(SERVER_FILE_DIRECTORY + username + DELIMITER + filename);
//                        file.delete();
//                    }catch (Exception e){
//                        e.printStackTrace();
//                    }
//
//                    System.out.println("download success");
//                } catch (FileNotFoundException e) {
//                    System.out.println("file not found");
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    System.out.println("other reason");
//                    e.printStackTrace();
//                }
//            }
//        };

        FileService.FileInfo fileInfo = FileService.FileInfo.newBuilder().setUsername(username).setFilename(filename).build();
        mAsyncStub.downloadFile(fileInfo, responseObserver);
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


    @SuppressWarnings("Duplicates")
    public void uploadFile(final String filepath, String userName) {
        logger.info("tid: " +  Thread.currentThread().getId() + ", Will try to getBlob");

        System.out.println(filepath);

        StreamObserver<FileService.ack> responseObserver = new StreamObserver<FileService.ack>() {

            @Override
            public void onNext(FileService.ack value) {
                System.out.println("upload file reponse :" + value.getSuccess());
                logger.info("Client response onNext");
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
                    FileService.FileData req = FileService.FileData.newBuilder().setUsername(userName).setFilename(file.getName()).setData(byteString).build();
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

    public void postLeaderInfo(String IP, int port, String clusterName) {
        FileService.ClusterInfo clusterInfo = FileService.ClusterInfo.newBuilder().setIp(IP)
                .setPort(String.valueOf(port))
                .setClusterName(clusterName).build();

        mAsyncStub.getLeaderInfo(clusterInfo, new StreamObserver<FileService.ack>() {
            @Override
            public void onNext(FileService.ack ack) {
                System.out.println("Post super node feedback : " + ack.getSuccess());
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("Super node error!" + throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Super node complete!");
            }
        });

//        FileService.ack result = mBlockingStub.getLeaderInfo(clusterInfo);
//        System.out.println("post Leadereader Infor feedback : " + result.getSuccess());
    }

    public void syncCall() {
        mBlockingStub.fileDelete(FileService.FileInfo.newBuilder().build());
    }
}
