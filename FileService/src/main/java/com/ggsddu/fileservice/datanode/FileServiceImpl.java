package com.ggsddu.fileservice.datanode;


import com.ggsddu.fileservice.MongoDBOperate;
import com.ggsddu.fileservice.datamodels.Server;
import com.google.protobuf.ByteString;
import fileservice.FileService;
import fileservice.FileserviceGrpc;
import io.grpc.stub.StreamObserver;
import org.bson.Document;

import java.io.*;
import java.util.List;

import static com.ggsddu.fileservice.Delimiter.DELIMITER;

public class FileServiceImpl extends FileserviceGrpc.FileserviceImplBase {
    private String mMessage = "";
    private BufferedOutputStream mBufferedOutputStream = null;
    private final static String FILE_DIRECTORY = "."+ DELIMITER +"node_files"+DELIMITER;

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
                if (mongoDBOperate.searchClientFile(username, filename)) {
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
                System.out.println("error : " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                MongoDBOperate mongoDBOperate = MongoDBOperate.getInstance();
                if (!mongoDBOperate.searchClientFile(username, filename)) {
                    Document document = new Document("username", username).
                            append("filename", filename).
                            append("path", FILE_DIRECTORY + username + DELIMITER + filename);
                    mongoDBOperate.insertClientFile(document);
                    responseObserver.onNext(FileService.ack.newBuilder().setSuccess(true).setMessage(mMessage).build());
                } else {
                    System.out.println("File already exist!!!");
                    responseObserver.onNext(FileService.ack.newBuilder().setSuccess(false).setMessage(mMessage).build());
                }

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
    public void downloadFile(FileService.FileInfo request, StreamObserver<FileService.FileData> responseObserver) {

        try {
            BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(FILE_DIRECTORY +request.getUsername() + DELIMITER + request.getFilename()));
            int bufferSize = 512 * 1024; // 512k
            byte[] buffer = new byte[bufferSize];
            int tmp = 0;
            int size = 0;
            while ((tmp = bInputStream.read(buffer)) > 0) {
                size += tmp;
                ByteString byteString = ByteString.copyFrom(buffer);
                FileService.FileData req = FileService.FileData.newBuilder().setUsername(request.getUsername()).setFilename(request.getFilename()).setData(byteString).build();
                responseObserver.onNext(req);
            }
            responseObserver.onCompleted();
            bInputStream.close();
            System.out.println("download success");
        } catch (FileNotFoundException e) {
            System.out.println("file not found");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("other reason");
            e.printStackTrace();
        }
    }

    @Override
    public void fileSearch(FileService.FileInfo request, StreamObserver<FileService.ack> responseObserver) {
//        super.fileSearch(request, responseObserver);

        String userName = request.getUsername();
        String fileName = request.getFilename();

        Boolean getFile = MongoDBOperate.getInstance().searchClientFile(userName, fileName);
        FileService.ack ac = FileService.ack.newBuilder().setSuccess(getFile).setMessage("test").build();
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
        List<String> filenames = mongoDBOperate.showClientAllFiles(username);
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

        try {
            File file = new File(FILE_DIRECTORY + userName + DELIMITER + fileName);

            if(file.delete()) {
                System.out.println( file.getName() + " is deleted!");

                Boolean isSuccess = MongoDBOperate.getInstance().deleteClientFile(userName, fileName);
                FileService.ack ack = FileService.ack.newBuilder().setSuccess(true).build();
                responseObserver.onNext(ack);
                responseObserver.onCompleted();

            }else {
                System.out.println("Delete operation is failed.");

                FileService.ack ack = FileService.ack.newBuilder().setSuccess(false).build();
                responseObserver.onNext(ack);
                responseObserver.onCompleted();
            }
        } catch(Exception e) {
            System.out.println("有异常.");
            e.printStackTrace();
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
                System.out.println("Upload complete");
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
}