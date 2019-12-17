package com.innoveller.dbunitdemo.helpers;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;

import java.util.*;

public class TableBuilder {
    private List<Column> columnList;
    private List<Object[]> objectArrayList;
    private String tableName;

    private TableBuilder(){
        columnList = new ArrayList<Column>();
        objectArrayList = new ArrayList<Object[]>();
    }

    private TableBuilder(Column[] columns) {
        columnList = new ArrayList<Column>();
        columnList.addAll(Arrays.asList(columns));
        objectArrayList = new ArrayList<Object[]>();
    }

    public static TableBuilder columns() {
        return new TableBuilder();
    }

    public static TableBuilder columns(Column[] columns) {
        return new TableBuilder(columns);
    }

    public TableBuilder add(String name, DataType dataType){
        columnList.add(new Column(name, dataType));
        return this;
    }

    public org.dbunit.dataset.Column[] buildColumns(){
        return columnList.toArray(new Column[columnList.size()]);
    }

    public static TableBuilder rows(String tableName, Column[] columns){
        TableBuilder tableBuilder = new TableBuilder();
        tableBuilder.tableName = tableName;
        tableBuilder.columnList.addAll(Arrays.asList(columns));
        return tableBuilder;
    }

    public TableBuilder add(Object... objects){
        objectArrayList.add(objects);
        return this;
    }

    public DefaultTable buildTable() throws DataSetException {
        DefaultTable defaultTable = new DefaultTable(tableName, buildColumns());
        for(Object[] objects: objectArrayList){
            defaultTable.addRow(objects);
        }
        return defaultTable;
    }

    /*public static class Columns {
        private TableBuilder column;

        public Columns(){
            column = new TableBuilder();
            column.columnMap = new LinkedHashMap<String, DataType>();
        }

        public Columns add(String name, DataType dataType){
            column.columnMap.put(name, dataType);
            return this;
        }

        public org.dbunit.dataset.Column[] build(){
            org.dbunit.dataset.Column[] columnArray = new org.dbunit.dataset.Column[column.columnMap.size()];
            int index = 0;
            for (Map.Entry<String,DataType> entry : column.columnMap.entrySet()){
                column.columnMap.size();
                org.dbunit.dataset.Column newColumn = new org.dbunit.dataset.Column(entry.getKey(), entry.getValue());
                columnArray[index] = newColumn;
                index++;
            }
            return columnArray;
        }
    }

    public static class Rows{
        private TableBuilder row;
        private DefaultTable defaultTable;

        public Rows(String tableName, Column[] columns){
            row = new TableBuilder();
            row.objectArrayList = new ArrayList<Object[]>();
            defaultTable = new DefaultTable(tableName, columns);
        }

        public Rows add(Object... objects){
            row.objectArrayList.add(objects);
            return this;
        }

        public DefaultTable build() throws DataSetException {
            for(int i=0; i<row.objectArrayList.size(); i++){
                defaultTable.addRow(row.objectArrayList.get(i));
            }
            return defaultTable;
        }
    }*/
}