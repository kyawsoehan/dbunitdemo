package com.innoveller.dbunitdemo.helpers;

import java.io.*;

public class FileReaderHelper {

    public String readSql (String pathName, String testName) throws IOException {

        File file = new File(pathName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        Boolean start = false;
        String sqlString = "";
        String st;
        int testNo = 0;

        while ((st = bufferedReader.readLine()) != null) {

            //System.out.println(testName + " --- " + st.contains(testName) +" ---- "+ st);
            if(st.contains("--<test:") && st.contains("name:"+testName)) {
                int testIndex = st.indexOf("test:");
                int nameIndex = st.indexOf("name:");
                testNo = Integer.parseInt(st.substring(testIndex+5, nameIndex-1));

                sqlString += st + "\n";
                start = true;
                //System.out.println("In while loop");
            }
            else if(st.contains("-->test:" + testNo)) {
                if(st.contains("sql:")) {
                    int index = st.indexOf("sql:");
                    //System.out.println("Substring --- " + st.substring(index+2));
                    sqlString += st.substring(index+4);
                    break;
                }
                start = false;
            }
            else if(start && st.contains("--")) {
                sqlString += st + "\n";
            }
            else if(start && !st.contains("--")) {
                sqlString += st + " ";
            }
        }

        //System.out.println("SqlString : " + sqlString);
        return sqlString;
    }

    /*public static void main (String args[]) throws IOException {
        System.out.println("Query result : "+ readSql(
                "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableRoomTypeInfo.sql",
                        "getHotelNRates"));
    }*/
}
