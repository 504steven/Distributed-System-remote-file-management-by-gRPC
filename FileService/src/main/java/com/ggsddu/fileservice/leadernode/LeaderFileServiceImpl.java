package com.ggsddu.fileservice.leadernode;

import com.ggsddu.fileservice.MongoDBOperate;
import com.ggsddu.fileservice.datamodels.Server;
import fileservice.FileService;
import fileservice.FileserviceGrpc;
import io.grpc.stub.StreamObserver;
import org.bson.Document;

import java.io.*;
import java.util.List;

import static com.ggsddu.fileservice.Delimiter.DELIMITER;

class LeaderFileServiceImpl extends FileserviceGrpc.FileserviceImplBase {
    private String mMessage = "";
    private BufferedOutputStream mBufferedOutputStream = null;
    private final static String FILE_DIRECTORY = "."+ DELIMITER +"server_files" + DELIMITER;

    @Override
    @SuppressWarnings("Duplicates")
    public StreamObserver<FileService.FileData> uploadFile(final StreamObserver<FileService.ack> responseObserver) {
        return new StreamObserver<FileService.FileData>() {
            int mmCount = 0;
            String username;
            String filename;
            @Override
            public void onNext(FileService.FileData request) {
                // Print count
                System.out.println("onNext count: " + mmCount);
                mmCount++;

                byte[] data = request.getData().toByteArray();
                username = request.getUsername();
                filename = request.getFilename();

                MongoDBOperate mongoDBOperate = MongoDBOperate.getInstance();
                if (mongoDBOperate.searchServerFile(username, filename)) {
                    System.out.println("File already exist!!!");
                    return;
                }

                // Create directory if not exist
                File dir = new File(FILE_DIRECTORY);
                dir.mkdir();
                dir = new File(FILE_DIRECTORY + username);
                dir.mkdir();

                try {
                    if (mBufferedOutputStream == null) {
                        mBufferedOutputStream = new BufferedOutputStream(new FileOutputStream(FILE_DIRECTORY + username + DELIMITER + filename));
                    }
                    mBufferedOutputStream.write(data);
                    mBufferedOutputStream.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

                if (mBufferedOutputStream != null) {
                    try {
                        mBufferedOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        mBufferedOutputStream = null;
                    }
                }


                MongoDBOperate mongoDBOperate = MongoDBOperate.getInstance();
                if (!mongoDBOperate.searchServerFile(username, filename)) {
                    responseObserver.onNext(FileService.ack.newBuilder().setSuccess(true).setMessage("File successfully uploaded").build());
                    responseObserver.onCompleted();
                } else {
                    System.out.println("File already exist!!!");
                    responseObserver.onNext(FileService.ack.newBuilder().setSuccess(false).setMessage("File upload failed").build());
                    responseObserver.onCompleted();
                    return;
                }

                //获取节点信息并写入到数据库
                List<Server> servers = MongoDBOperate.getInstance().getLeastUtilizedNodes();
                if(servers.size() != 0) {
                    Document document = null;
                    Server server = servers.get(0);
                    if (servers.size() == 1) {
                        document = new Document("username", username).
                                append("filename", filename).
                                append("server_id", server.getId());
                    } else {
                        Server back_server = servers.get(1);
                        document = new Document("username", username).
                                append("filename", filename).
                                append("server_id", server.getId()).
                                append("backup_server_id", back_server.getId());
                    }
                    mongoDBOperate.insertServerFile(document);
                }

                //传文件到子节点
                for(Server server : servers){
                    System.out.println(server.getIp());
                    LeaderForwardFile client = new LeaderForwardFile(server.getIp(), server.getFile_port());
                    try {
                        client.uploadFile(FILE_DIRECTORY + username + DELIMITER + filename, username);
                        try {
                            File file = new File(FILE_DIRECTORY + username + DELIMITER + filename);
                            file.delete();
                        }catch (Exception e){
                            e.printStackTrace();
                        }

                    } finally {
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        try {
                            client.shutdown();
                        } catch (InterruptedException e){
                            e.printStackTrace();
                        }
                    }
                }

                try {
                    File file = new File(FILE_DIRECTORY + username + "/" + filename);
                    file.delete();
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        };
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void downloadFile(FileService.FileInfo request, StreamObserver<FileService.FileData> responseObserver) {
        //从datanode获取数据，然后再传到客户端
        String userName = request.getUsername();
        String fileName = request.getFilename();

//        Server server = MongoDBOperate.getInstance().getServerInfo(userName, fileName);

        List<String> list = MongoDBOperate.getInstance().getTwoNodeServer(userName, fileName);

        if(list == null){
            System.out.println("No such file");
            responseObserver.onError(new Throwable());
//            responseObserver.onCompleted();
            return;
        }

        String server_id = null;
        if(MongoDBOperate.getInstance().isGoodServer(list.get(0))){
            server_id = list.get(0);
            System.out.println("The main node fine, you can get file");
        }else if(MongoDBOperate.getInstance().isGoodServer(list.get(1))){
            System.out.println("The main node is broken but backup node is fine, you can get file");
            server_id = list.get(1);
        }else{
            System.out.println("all server is broken");
            responseObserver.onError(new Throwable());
            return;
        }
//        if(MongoDBOperate.getInstance().isGoodServer(list.get(0))){
//            Server = MongoDBOperate.getInstance().getServerInfo(list.get(0));
//        }
        Server server = MongoDBOperate.getInstance().getServerInfo(server_id);

        LeaderForwardFile client = new LeaderForwardFile(server.getIp(), server.getFile_port());
        try {
//            client.uploadFile(FILE_DIRECTORY + username + "/" + filename);
            client.downloadFile(request.getFilename(), request.getUsername(), responseObserver);
        } finally {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                client.shutdown();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }

//        try {
//            BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(FILE_DIRECTORY +request.getUsername() + "/"+ request.getFilename()));
//            int bufferSize = 512 * 1024; // 512k
//            byte[] buffer = new byte[bufferSize];
//            int tmp = 0;
//            int size = 0;
//            while ((tmp = bInputStream.read(buffer)) > 0) {
//                size += tmp;
//                ByteString byteString = ByteString.copyFrom(buffer);
//                FileData req = FileData.newBuilder().setUsername(request.getUsername()).setFilename(request.getFilename()).setData(byteString).build();
//                responseObserver.onNext(req);
//            }
//            responseObserver.onCompleted();
//            bInputStream.close();
//            System.out.println("download success");
//        } catch (FileNotFoundException e) {
//            System.out.println("file not found");
//            e.printStackTrace();
//        } catch (IOException e) {
//            System.out.println("other reason");
//            e.printStackTrace();
//        }
    }

    @Override
    public void fileSearch(FileService.FileInfo request, StreamObserver<FileService.ack> responseObserver) {
//        super.fileSearch(request, responseObserver);

        String userName = request.getUsername();
        String fileName = request.getFilename();

        Boolean getFile = MongoDBOperate.getInstance().searchServerFile(userName, fileName);
        String message  = null;
        if(getFile){
            message = "success";
        }else{
            message = "fail";
        }
        FileService.ack ac = FileService.ack.newBuilder().setSuccess(getFile).setMessage(message).build();
        responseObserver.onNext(ac);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<FileService.FileData> replicateFile(StreamObserver<FileService.ack> responseObserver) {
        return super.replicateFile(responseObserver);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void fileList(FileService.UserInfo request, StreamObserver<FileService.FileListResponse> responseObserver) {
        String username = request.getUsername();
        MongoDBOperate mongoDBOperate = MongoDBOperate.getInstance();
        List<String> filenames = mongoDBOperate.showServerAllFiles(username);
        StringBuilder sb = new StringBuilder();
        for (String filename : filenames) {
            sb.append(filename + ",");
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }

        FileService.FileListResponse reply = FileService.FileListResponse.newBuilder().setFilenames(sb.toString()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void fileDelete(FileService.FileInfo request, StreamObserver<FileService.ack> responseObserver) {
        System.out.println("recieve delete file request.");

        String userName = request.getUsername();
        String fileName = request.getFilename();

        List<String> list = MongoDBOperate.getInstance().getTwoNodeServer(userName, fileName);
        if(list == null){
            System.out.println("No such file");

            FileService.ack ack = FileService.ack.newBuilder().setSuccess(false).build();
            responseObserver.onNext(ack);
            responseObserver.onCompleted();
        }else {

            Boolean isSuccess = MongoDBOperate.getInstance().deleteServerFile(userName, fileName);
            if(!isSuccess){
                System.out.println("Delete operation is failed.");

                FileService.ack ack = FileService.ack.newBuilder().setSuccess(false).build();
                responseObserver.onNext(ack);
                responseObserver.onCompleted();
            }else {

                for (String server_id : list) {
                    if (isSuccess) {
                        System.out.println(fileName + " is deleted!");

                        //让子节点删除数据
                        Server server = MongoDBOperate.getInstance().getServerInfo(server_id);
                        LeaderForwardFile client = new LeaderForwardFile(server.getIp(), server.getFile_port());
                        try {
                            client.fileDelete(userName, fileName);
                        } finally {
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            try {
                                client.shutdown();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    FileService.ack ack = FileService.ack.newBuilder().setSuccess(true).build();
                    responseObserver.onNext(ack);
                    responseObserver.onCompleted();
                }
            }

        }



    }


    public void getLeaderInfo(FileService.ClusterInfo request,
                              io.grpc.stub.StreamObserver<FileService.ack> responseObserver) {
        String node_ip = request.getIp();
        String node_port = request.getPort();
        String node_heatBeatPort = request.getClusterName();

        if(MongoDBOperate.getInstance().ipExist(node_ip)){
            FileService.ack ack = FileService.ack.newBuilder().setSuccess(false).setMessage("ip has exist").build();
            responseObserver.onNext(ack);
            responseObserver.onCompleted();
        }else {
            long n = MongoDBOperate.getInstance().nodeCount();
            Document document = new Document("id", String.valueOf(n)).append("ip", node_ip).append("file_port", Integer.valueOf(node_port)).
                    append("heartbeat_port", 50052);
            MongoDBOperate.getInstance().intsertNodeInfo(document);
            FileService.ack ack = FileService.ack.newBuilder().setSuccess(true).setMessage("success").build();
            responseObserver.onNext(ack);
            responseObserver.onCompleted();
        }
    }

    @Override
    @SuppressWarnings("Duplicates")
    public StreamObserver<FileService.FileData> updateFile(final StreamObserver<FileService.ack> responseObserver) {
        return new StreamObserver<FileService.FileData>() {
            int mmCount = 0;

            String username;
            String filename;

            @Override
            public void onNext(FileService.FileData request) {
                // Print count
                System.out.println("onNext count: " + mmCount);
                mmCount++;

                byte[] data = request.getData().toByteArray();
                username = request.getUsername();
                filename = request.getFilename();

                // Create directory if not exist
                File dir = new File(FILE_DIRECTORY);
                dir.mkdir();
                dir = new File(FILE_DIRECTORY + username);
                dir.mkdir();

                try {
                    if (mBufferedOutputStream == null) {
                        mBufferedOutputStream = new BufferedOutputStream(new FileOutputStream(FILE_DIRECTORY + username + DELIMITER + filename));
                    }
                    mBufferedOutputStream.write(data);
                    mBufferedOutputStream.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(FileService.ack.newBuilder().setSuccess(true).setMessage(mMessage).build());
                responseObserver.onCompleted();
                if (mBufferedOutputStream != null) {
                    try {
                        mBufferedOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        mBufferedOutputStream = null;
                    }
                }
            }
        };
    }


    @Override
    public void getClusterStats(FileService.Empty request, StreamObserver<FileService.ClusterStats> responseObserver) {
        List<Server> serverList = MongoDBOperate.getInstance().getAllServers();
        double cpuUsage = 0.0;
        double memUsage = 0.0;
        double diskUsage = 0.0;
        int nodeNum = serverList.size();
        for(Server server : serverList) {
            if (server.getMem_usage() == null || server.getDisk_usage() == null || server.getCpu_usage() == null) {
                continue;
            }
            cpuUsage += Double.valueOf( server.getCpu_usage());
            memUsage += Double.valueOf( server.getMem_usage());
            diskUsage += Double.valueOf( server.getDisk_usage());
        }
        cpuUsage /= nodeNum;
        memUsage /= nodeNum;
        diskUsage /= nodeNum;

        responseObserver.onNext( FileService.ClusterStats.newBuilder().setCpuUsage( String.valueOf(cpuUsage))
                .setUsedMem( String.valueOf(memUsage))
                .setDiskSpace( String.valueOf(diskUsage)).build());
        responseObserver.onCompleted();
    }
}