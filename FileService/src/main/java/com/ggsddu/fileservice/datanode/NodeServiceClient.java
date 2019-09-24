package com.ggsddu.fileservice.datanode;

import fileservice.FileService;
import fileservice.FileserviceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

public class NodeServiceClient {
    // Config current node info
//    private static final String IP = FileServiceServer.;
//    private static final String  FILE_PORT = "60051";

    private final ManagedChannel mChannel;
    private final FileserviceGrpc.FileserviceBlockingStub mBlockingStub;
    private final FileserviceGrpc.FileserviceStub mAsyncStub;

    public NodeServiceClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    NodeServiceClient(ManagedChannel channel) {
        this.mChannel = channel;
        mBlockingStub = FileserviceGrpc.newBlockingStub(channel);
        mAsyncStub = FileserviceGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void transferNodeInfo(){
        StreamObserver<FileService.ack> responseObserver = new StreamObserver<FileService.ack>() {
            @Override
            public void onNext(FileService.ack value) {
                if (value.getSuccess()) {
                    System.out.println("Transfer node info success");
                } else {
                    System.out.println("Transfer node info failed");
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };

        FileService.ClusterInfo nodeInfo = FileService.ClusterInfo.newBuilder().setIp(FileServiceServer.DATA_NODE_IP).setPort(String.valueOf(FileServiceServer.DATA_NODE_PORT)).
                setClusterName("datanode").build();
        mAsyncStub.getLeaderInfo(nodeInfo, responseObserver);
        FileService.ack ack = mBlockingStub.getLeaderInfo(nodeInfo);
        System.out.println("post leader infor responce: " + ack.getSuccess());
    }

//    public static void main(String[] args) throws Exception {
//        String SERVER_IP = "localhost";
//        int SERVER_PORT = 50051;
//        NodeServiceClient client = new NodeServiceClient(SERVER_IP, SERVER_PORT);
//        NodeServiceClient client = new NodeServiceClient("localhost", 50051);
//            client.transferNodeInfo();
//        } finally {
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            client.shutdown();
//        }
//    }
}
