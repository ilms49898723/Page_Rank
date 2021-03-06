package com.github.ilms49898723.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Cleaner {
    public static void start(String output) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.delete(new Path(output), true);
            fileSystem.delete(new Path("/user/root/prematrix"), true);
            fileSystem.delete(new Path("/user/root/matrixparse"), true);
            fileSystem.delete(new Path("/user/root/matrixtrans"), true);
            fileSystem.delete(new Path("/user/root/matrixmul"), true);
            fileSystem.delete(new Path("/user/root/mapping"), true);
            fileSystem.delete(new Path("/user/root/indices"), true);
            fileSystem.delete(new Path("/user/root/R"), true);
            fileSystem.delete(new Path("/user/root/Rindex"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void remove(String dir) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.delete(new Path(dir), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
