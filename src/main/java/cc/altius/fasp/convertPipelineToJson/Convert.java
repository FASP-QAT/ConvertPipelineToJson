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
public class Convert {

    // Clean build using mvn clean compile assembly:single
    public static void main(String[] args) throws IOException {
        String fileName = "/home/akil/db/globalmoh.accdb";
        if (args.length > 0) {
            fileName = args[0];
        }
        String databaseURL = "jdbc:ucanaccess://" + fileName + ";openExclusive=false;ignoreCase=true";
        Date dt = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        File f = new File("pipelineDb-" + sdf.format(dt) + ".json");
        FileWriter fw = new FileWriter(f);
        StringBuilder sb = new StringBuilder().append("{");
        try (Connection connection = DriverManager.getConnection(databaseURL)) {
            String sql = "select [TABLE_NAME] From information_schema.tables where table_schema='PUBLIC'";
            Statement statement = connection.createStatement();
            ResultSet resultTables = statement.executeQuery(sql);
            while (resultTables.next()) {
                String tableName = resultTables.getString(1).toLowerCase();
                sb.append("\"").append(tableName).append("\":[");
                String sqlData = "SELECT * FROM [" + resultTables.getString(1) + "]";
                Statement statementData = connection.createStatement();
                ResultSet resultData = statementData.executeQuery(sqlData);
                ResultSetMetaData rsmdData = resultData.getMetaData();
                int rowCount = 0;
                while (resultData.next()) {
                    for (int y = 1; y <= rsmdData.getColumnCount(); y++) {
                        if (y == 1) {
                            sb.append("{");
                        }
                        if (y > 1) {
                            sb.append(",");
                        }
                        String data = resultData.getString(y);
                        sb.append("\"").append(rsmdData.getColumnName(y).toLowerCase().replace(" ", "_")).append("\"").append(":").append(encapsulateData(rsmdData.getColumnTypeName(y), data));
                    }
                    sb.append("},");
                    rowCount++;
                }
                if (rowCount != 0) {
                    sb.setLength(sb.length() - 1);
                }
                sb.append("],");
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        sb.setLength(sb.length() - 1);
        sb.append("}");
        fw.append(sb);
        fw.close();
    }

    private static String encapsulateData(String dataType, String data) {
        if (data == null) {
            return null;
        } else {
            switch (dataType) {
                case "VARCHAR":
                    data = data.replaceAll("\\\\","\\\\\\\\");
                    data = data.replaceAll("\n", " ").replaceAll("\r", " ").replaceAll("\"", "`").replaceAll("'", "`");
                    return "\"" + data + "\"";
                case "DOUBLE":
                case "INTEGER":
                case "SMALLINT":
                    return data;
                case "TIMESTAMP":
                    return "\"" + data.substring(0, 26).replace(' ', 'T') + "\"";
                case "BOOLEAN":
                    return data.toLowerCase();
                default:
                    return "Unkown";
            }
        }
    }
}
