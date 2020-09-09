package com.linkoopdb.datax.plugin;

import com.alibaba.datax.plugin.linkoopdb.util.LinkoopDBUtils;
import javax.imageio.stream.FileImageInputStream;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Scanner;

public class ClobAndBlobTest {
    public static void main(String[] args) {
        String url = "jdbc:linkoopdb:tcp://test91:9105/ldb;query_iterator=1";
        String driver = "com.datapps.linkoopdb.jdbc.JdbcDriver";
        String username = "admin";
        String password = "123456";
        LinkoopDBUtils.init(username, password, driver, url);

        String location = "/Users/vzhzhq/Documents/work/code/DataX-Linkoopdb/plugin-linkoopdb-util/src/main/assembly";

        String schema = "DATATRANS";

//        String tableName = "ClobTest";
//        String type = "info clob";

        String tableName = "BlobTest";
        String type = "info blob";

        String createsql = "CREATE TABLE IF NOT EXISTS " + schema + "." + tableName + "(" + type + ") engine pallas;";
        LinkoopDBUtils.createTable(createsql);
        String sql  = "insert into " + schema + "." + tableName + " values(?);";

        Connection connection = LinkoopDBUtils.getConnection();

        int ret = 0;
        try {
            int i = 1;

//            InputStream inputStream = new FileInputStream(location + File.separator + "追风筝的人_第二章.txt");
//            Scanner scanner = new Scanner(inputStream,"UTF-8");
//            String text = scanner.useDelimiter("\\A").next();
//            PreparedStatement statement = connection.prepareStatement(sql);
//            statement.setClob(i, new SerialClob(text.toCharArray()));
//            ret = statement.executeUpdate();

            String path = location + File.separator + "追风筝的人_第二章.txt";
            byte[] bytes = imageToByte(path);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setBlob(i, new SerialBlob(bytes));
            ret = statement.executeUpdate();

            LinkoopDBUtils.closeConnection(connection, null, statement);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("get shard info error!");
        }
    }

    public static byte[] imageToByte(String path){
        byte[] data = null;
        FileImageInputStream input = null;
        try {
            input = new FileImageInputStream(new File(path));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int numBytesRead = 0;
            while ((numBytesRead = input.read(buf)) != -1) {
                output.write(buf, 0, numBytesRead);
            }
            data = output.toByteArray();
            output.close();
            input.close();
        }
        catch (FileNotFoundException ex1) {
            ex1.printStackTrace();
        }
        catch (IOException ex1) {
            ex1.printStackTrace();
        }
        return data;
    }
}
