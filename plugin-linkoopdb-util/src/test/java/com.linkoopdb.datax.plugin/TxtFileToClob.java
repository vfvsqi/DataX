package com.linkoopdb.datax.plugin;

import com.csvreader.CsvReader;
import java.io.*;

public class TxtFileToClob {
    public static void main(String[] args) {
        String fileName = "/Users/vzhzhq/Documents/work/code/DataX-Linkoopdb/datax/bin/txt/a.txt";
//        RecordSender recordSender;
        String encoding = "UTF-8";
        InputStream inputStream;
        int bufferSize = 8096;
        char fieldDelimiter = ',';
        try {
            inputStream = new FileInputStream(fileName);
            InputStreamReader in = new InputStreamReader(inputStream, encoding);
            BufferedReader reader = new BufferedReader(in, bufferSize);

            CsvReader csvReader  = null;
            csvReader = new CsvReader(reader);
            csvReader.setDelimiter(fieldDelimiter);

//            setCsvReaderConfig(csvReader);
            String[] parseRows;
            while ((parseRows = splitBufferedReader(csvReader)) != null) {
                System.out.println("--------------------------------------");
                logString(parseRows);
                System.out.println("--------------------------------------");
//                UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
//                        column, parseRows, nullFormat, taskPluginCollector);
            }

//            recordSender.flush();
        } catch (Exception e) {
            // warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
            String message = String
                    .format("找不到待读取的文件 : [%s]", fileName);
            System.out.println(message);
        }
    }

    public static String[] splitBufferedReader(CsvReader csvReader)
            throws IOException {
        String[] splitedResult = null;
        if (csvReader.readRecord()) {
            splitedResult = csvReader.getValues();
        }
        return splitedResult;
    }

    public static void logString(String[] list) {
        for (int i = 0; i < list.length; i++) {
            System.out.println(list[i]);
        }
    }

}
