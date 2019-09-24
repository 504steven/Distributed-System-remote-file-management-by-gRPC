package com.ggsddu.fileservice.datamodels;

public class Server {
    private String id;
    private String ip;
    private int file_port;
    private int heartbeat_port;
    private String cpu_usage;
    private String mem_usage;
    private String disk_usage;


    public Server(String id, String ip, int file_port, int heartbeat_port, String cpu_usage, String mem_usage, String disk_usage) {
        this.id = id;
        this.ip = ip;
        this.file_port = file_port;
        this.heartbeat_port = heartbeat_port;
        this.cpu_usage = cpu_usage;
        this.mem_usage = mem_usage;
        this.disk_usage = disk_usage;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getFile_port() {
        return file_port;
    }

    public void setFile_port(int file_port) {
        this.file_port = file_port;
    }

    public int getHeartbeat_port() {
        return heartbeat_port;
    }

    public void setHeartbeat_port(int heartbeat_port) {
        this.heartbeat_port = heartbeat_port;
    }

    public String getCpu_usage() {
        return cpu_usage;
    }

    public void setCpu_usage(String cpu_usage) {
        this.cpu_usage = cpu_usage;
    }

    public String getMem_usage() {
        return mem_usage;
    }

    public void setMem_usage(String mem_usage) {
        this.mem_usage = mem_usage;
    }

    public String getDisk_usage() {
        return disk_usage;
    }

    public void setDisk_usage(String disk_usage) {
        this.disk_usage = disk_usage;
    }
}