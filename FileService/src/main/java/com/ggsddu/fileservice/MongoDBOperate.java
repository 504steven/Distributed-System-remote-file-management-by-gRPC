package com.ggsddu.fileservice;

import com.ggsddu.fileservice.datamodels.Server;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MongoDBOperate {

    private static MongoDBOperate instance = new MongoDBOperate();
    private MongoCollection<Document> client_file_collection;

    private MongoCollection<Document> server_file_collection;
    private MongoCollection<Document> server_collection;

    private MongoDBOperate(){
        MongoClient mongoClient = MongoClients.create();
        MongoDatabase database = mongoClient.getDatabase("cmpe275");
        client_file_collection = database.getCollection("client_file");

        server_file_collection = database.getCollection("server_file");
        server_collection = database.getCollection("server");
    }

    public static MongoDBOperate getInstance(){
        return instance;
    }

    public void insertClientFile(Document doc) {client_file_collection.insertOne(doc);}

    @SuppressWarnings("Duplicates")
    public boolean deleteClientFile(String userName, String fileName){
        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        object.put("filename", fileName);
        DeleteResult result = client_file_collection.deleteOne(object);
        System.out.println("delete count " + result.getDeletedCount());
        if(result.getDeletedCount() == 0){
            return false;
        }else{
            return true;
        }
    }

    @SuppressWarnings("Duplicates")
    public boolean searchClientFile(String userName, String fileName){

        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        object.put("filename", fileName);
        Document doc = client_file_collection.find(object).first();
        if(doc == null){
            return false;
        }else{
            System.out.println(doc.toJson());
            return true;
        }
    }

    public void updateClientFile(String oldName, String newName){
//        collection.updateOne(eq("name", oldName),
//                new Document("$set", new Document("name", newName)));
    }

    @SuppressWarnings("Duplicates")
    public List<String> showClientAllFiles(String userName){
        List<String> result = new ArrayList<String>();

        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        MongoCursor<Document> cursor = client_file_collection.find(object).iterator();
        try {
            while (cursor.hasNext()) {
                //System.out.println(cursor.next().toJson());
                //cursor.next().get("filename")
                //System.out.println(cursor.next().get("filename"));
                result.add(cursor.next().get("filename").toString());
            }
        } finally {
            cursor.close();
        }
        return result;
    }



    //所有操作server file的操作
    public void insertServerFile(Document doc) {server_file_collection.insertOne(doc);}

    @SuppressWarnings("Duplicates")
    public boolean searchServerFile(String userName, String fileName){
        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        object.put("filename", fileName);
        Document doc = server_file_collection.find(object).first();
        if(doc == null){
            return false;
        }else{
            System.out.println(doc.toJson());
            return true;
        }
    }

    public Server getServerInfo(String userName, String fileName){
        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        object.put("filename", fileName);
        Document doc = server_file_collection.find(object).first();
        if(doc == null){
            return null;
        }else{
//            System.out.println(doc.toJson());
//            return true;
            String id = doc.getString("server_id");
            BasicDBObject object1 = new BasicDBObject();
            object1.put("id", id);
            Document doc1 = server_collection.find(object1).first();
            if(doc1 == null)
                return null;
            else{
                Server server = new Server(
                        doc1.getString("id"),
                        doc1.getString("ip"),
                        doc1.getInteger("file_port"),
                        doc1.getInteger("heartbeat_port"),
                        doc1.getString("cpu_usage"),
                        doc1.getString("mem_usage"),
                        doc1.getString("disk_usage"));
                return server;
            }

        }
    }

    public Server getServerInfo(String server_id){
        BasicDBObject object1 = new BasicDBObject();
        object1.put("id", server_id);
        Document doc1 = server_collection.find(object1).first();
        if(doc1 == null)
            return null;
        else{
            Server server = new Server(
                    doc1.getString("id"),
                    doc1.getString("ip"),
                    doc1.getInteger("file_port"),
                    doc1.getInteger("heartbeat_port"),
                    doc1.getString("cpu_usage"),
                    doc1.getString("mem_usage"),
                    doc1.getString("disk_usage"));
            return server;
        }
    }

    public List<String> getTwoNodeServer(String userName, String fileName){
        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        object.put("filename", fileName);
        Document doc = server_file_collection.find(object).first();
        if(doc == null)
            return null;
        else{
            List<String> list = new ArrayList<String>();
            String server_id = doc.getString("server_id");
            list.add(server_id);

            String backup_id = doc.getString("backup_server_id");
            if(backup_id != null){
                list.add(backup_id);
            }
            return list;
        }
    }

    @SuppressWarnings("Duplicates")
    public boolean deleteServerFile(String userName, String fileName){
        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        object.put("filename", fileName);
        DeleteResult result = server_file_collection.deleteOne(object);
        System.out.println("delete count " + result.getDeletedCount());
        if(result.getDeletedCount() == 0){
            return false;
        }else{
            return true;
        }
    }

    @SuppressWarnings("Duplicates")
    public List<String> showServerAllFiles(String userName){
        List<String> result = new ArrayList<String>();

        BasicDBObject object = new BasicDBObject();
        object.put("username", userName);
        MongoCursor<Document> cursor = server_file_collection.find(object).iterator();
        try {
            while (cursor.hasNext()) {
                //System.out.println(cursor.next().toJson());
                //cursor.next().get("filename")
                //System.out.println(cursor.next().get("filename"));
                result.add(cursor.next().get("filename").toString());
            }
        } finally {
            cursor.close();
        }
        return result;
    }





    //所有操作server储存node的操作

    public void intsertNodeInfo(Document document){
        server_collection.insertOne(document);
    }

    public long nodeCount(){
        return server_collection.countDocuments();
    }

    public boolean ipExist(String ip){
        BasicDBObject object = new BasicDBObject();
        object.put("ip", ip);
        Document doc = server_collection.find(object).first();
        if(doc == null){
            return false;
        }else {
            return true;
        }
    }


    public List<Server> getAllServers(){
        List<Server> result = new ArrayList<Server>();

        MongoCursor<Document> cursor = server_collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                Document document = cursor.next();

                Server server = new Server(
                        document.getString("id"),
                        document.getString("ip"),
                        document.getInteger("file_port"),
                        document.getInteger("heartbeat_port"),
                        document.getString("cpu_usage"),
                        document.getString("mem_usage"),
                        document.getString("disk_usage")
                );
                result.add(server);
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    public List<Server> getLeastUtilizedNodes() {
        Comparator<Server> comparator = new Comparator<Server>() {
            @Override
            public int compare(Server o1, Server o2) {
                double ave1, ave2;
                try {
                    ave1 = (Double.valueOf(o1.getCpu_usage()) + Double.valueOf(o1.getDisk_usage()) + Double.valueOf(o1.getMem_usage())) / 3;
                } catch (NullPointerException e) {
                    return 1;
                }
                try {
                    ave2 = (Double.valueOf(o2.getCpu_usage()) + Double.valueOf(o2.getDisk_usage()) + Double.valueOf(o2.getMem_usage())) / 3;
                } catch (NullPointerException e) {
                    return -1;
                }
                return Double.compare(ave1, ave2);
            }
        };

        List<Server> servers = getAllServers();
        Collections.sort(servers, comparator);

        List<Server> results = new ArrayList<Server>();
        for (int i = 0; i < Math.min(2, servers.size()); i++) {
            if (servers.get(i).getMem_usage() == null || servers.get(i).getDisk_usage() == null || servers.get(i).getCpu_usage() == null) {
                break;
            }
            results.add(servers.get(i));
        }

        for (Server server : results) {
            System.out.println("Selected server: " + server.getId());
        }

        return results;
    }

    public void updateServerStats(Server server, String cpu_usage, String disk_usage, String mem_usage) {
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.append("id", server.getId());

        BasicDBObject object = new BasicDBObject();
        object.put("cpu_usage", cpu_usage);
        object.put("disk_usage", disk_usage);
        object.put("mem_usage", mem_usage);

        BasicDBObject updateQuery = new BasicDBObject();
        updateQuery.append("$set", object);
        UpdateResult result = server_collection.updateMany(searchQuery, updateQuery);
//        System.out.println(result.toString());
    }

    public boolean isGoodServer(String id) {
        BasicDBObject object = new BasicDBObject();
        object.put("id", id);
        Document document = server_collection.find(object).first();
        if (document == null || document.get("mem_usage") == null || document.get("cpu_usage") == null || document.get("disk_usage") == null) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
//        instance.getLeastUtilizedNodes();
        System.out.println(instance.isGoodServer("1"));
    }
}
