/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cc.altius.fasp.convertPipelineToJson;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author akil
 */
public class GetStructure {

    public static void main(String[] args) throws IOException {
        String fileName = "/home/akil/db/globalmoh.accdb";
        if (args.length > 0) {
            fileName = args[0];
        }
        String databaseURL = "jdbc:ucanaccess://" + fileName + ";openExclusive=false;ignoreCase=true";
        Date dt = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
//        File f = new File("pipelineDb-" + sdf.format(dt) + ".json");
//        FileWriter fw = new FileWriter(f);
//        StringBuilder sb = new StringBuilder().append("{");
        try (Connection connection = DriverManager.getConnection(databaseURL)) {
            String sql = "select [TABLE_NAME] From information_schema.tables where table_schema='PUBLIC'";
            Statement statement = connection.createStatement();
            ResultSet resultTables = statement.executeQuery(sql);
            System.out.println("Going to start with tables");
            while (resultTables.next()) {
                System.out.println("Table " + resultTables.getString(1));
                String sqlData = "SELECT * FROM [" + resultTables.getString(1) + "]";
                Statement statementData = connection.createStatement();
                ResultSet resultData = statementData.executeQuery(sqlData);
                ResultSetMetaData rsmdData = resultData.getMetaData();
                for (int y = 1; y <= rsmdData.getColumnCount(); y++) {
                    System.out.print(rsmdData.getColumnName(y) + "(" + rsmdData.getColumnTypeName(y) + "[" + rsmdData.getColumnDisplaySize(y) + "])" + " | ");
                }
                System.out.println("");
                System.out.println("");
            }
            System.out.print("\n");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
//        sb.setLength(sb.length() - 1);
//        sb.append("}");
//        fw.append(sb);
//        fw.close();
    }

}
