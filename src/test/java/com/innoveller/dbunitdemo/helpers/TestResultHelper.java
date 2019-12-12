package com.innoveller.dbunitdemo.helpers;

import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlWriter;

import java.io.FileOutputStream;

public class TestResultHelper {
    public static void saveResults(ITable expectedTable, ITable resultTable) {
        try {
            DefaultDataSet dataSet = new DefaultDataSet();
            dataSet.addTable(expectedTable);
            dataSet.addTable(resultTable);

            FlatXmlWriter writer = new FlatXmlWriter(new FileOutputStream("results/temp.xml"));
            writer.write(dataSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
