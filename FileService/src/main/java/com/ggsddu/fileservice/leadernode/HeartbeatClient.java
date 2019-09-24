package com.ggsddu.fileservice.leadernode;

import com.ggsddu.fileservice.MongoDBOperate;
import com.ggsddu.fileservice.datamodels.Server;
import fileservice.FileService;
import fileservice.FileserviceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HeartbeatClient {

    private static final Logger logger = Logger.getLogger(HeartbeatClient.class.getName());

    private final ManagedChannel mChannel;
    private final FileserviceGrpc.FileserviceBlockingStub mBlockingStub;
    private final FileserviceGrpc.FileserviceStub mAsyncStub;

    public HeartbeatClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    HeartbeatClient(ManagedChannel channel) {
        this.mChannel = channel;
        mBlockingStub = FileserviceGrpc.newBlockingStub(channel);
        mAsyncStub = FileserviceGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public FileService.ClusterStats getClusterStats(){
        FileService.Empty request = FileService.Empty.newBuilder().build();
        FileService.ClusterStats response;

        try {
            response = mBlockingStub.getClusterStats(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        System.out.println("   CPU:" + response.getCpuUsage() + " MEM:" + response.getUsedMem() + " DISK:" + response.getDiskSpace());
        return response;
    }

    public static void main(String[] args) throws Exception {
        MongoDBOperate mongoDBOperate = MongoDBOperate.getInstance();
        while (true) {
            List<Server> servers = mongoDBOperate.getAllServers();
            for (Server server : servers) {
                HeartbeatClient client = new HeartbeatClient(server.getIp(), server.getHeartbeat_port());
                try {
                    FileService.ClusterStats response = client.getClusterStats();
                    mongoDBOperate.updateServerStats(server, response.getCpuUsage(), response.getDiskSpace(), response.getUsedMem());
                } catch (Exception e) {
                    mongoDBOperate.updateServerStats(server, null, null, null);
                } finally {
                    client.shutdown();
                }
            }
            Thread.sleep(1000);
        }
    }
}
