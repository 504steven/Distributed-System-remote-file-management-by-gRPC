package com.ggsddu.fileservice;

public class Delimiter {
    public static String DELIMITER = "/";

    public Delimiter() {
        String os = System.getProperty("os.name").toLowerCase();
        if(os.startsWith("windows")) {
            DELIMITER = "\\";
        }
        System.out.println(os);
    }

    public static void main(String[] args) {
        new Delimiter();
    }

}
