package com.alibaba.datax.plugin.writer.excelwriter;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExcelWriteTest {

    private static final List<Employee> employees =  new ArrayList<>();

    // Initializing employees data to insert into the excel file
    static {
        employees.add(new Employee("Rajeev Singh", "rajeev@example.com", 1200000.0));
        employees.add(new Employee("Thomas cook", "thomas@example.com", 1500000.0));
        employees.add(new Employee("Steve Maiden", "steve@example.com", 1800000.0));
    }

    public static void main(String[] args) throws IOException {
        // Create a Workbook
        // new HSSFWorkbook() for generating `.xls` file
        Workbook workbook = new XSSFWorkbook();

        /* CreationHelper helps us create instances of various things like DataFormat,
           Hyperlink, RichTextString etc, in a format (HSSF, XSSF) independent way */
        CreationHelper createHelper = workbook.getCreationHelper();

        // Create a Sheet
        Sheet sheet = workbook.createSheet("Employee");

        // Create Other rows and cells with employees data
        int rowNum = 0;
        for(Employee employee: employees) {
            Row row = sheet.createRow(rowNum++);

            row.createCell(0)
                    .setCellValue(employee.getName());

            row.createCell(1)
                    .setCellValue(employee.getEmail());

            row.createCell(2)
                    .setCellValue(employee.getSalary());
        }

        // Resize all columns to fit the content size
//        for(int i = 0; i < columns.length; i++) {
//            sheet.autoSizeColumn(i);
//        }

        // Write the output to a file
        FileOutputStream fileOut = new FileOutputStream("poi-generated-file.xlsx");
        workbook.write(fileOut);
        fileOut.close();

        // Closing the workbook
        workbook.close();
    }
}
