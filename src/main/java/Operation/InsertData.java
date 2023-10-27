package Operation;

import entity.Convert;
import util.DBUtil;
import util.JsonToMap;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class InsertData {
//    public static void main(String[] args) throws ClassNotFoundException, SQLException {
//        Class.forName("com.informix.jdbc.IfxDriver");
//        JsonToMap jsonToMap = new JsonToMap();
//        File configFile = new File("C:\\Users\\Administrator\\Desktop\\config.txt");
//        Convert convert = jsonToMap.jsonToMap(configFile);
//        DBUtil dbUtil = new DBUtil(convert);
//        Connection connection = dbUtil.getConn();
//        for (int i = 10000;i < 99999 ; i++){
//            String sql = "insert into f_chn_history_photo_feature values(?,?,?,?)";
//            PreparedStatement preparedStatement = connection.prepareStatement(sql);
//            preparedStatement.setString(1,String.valueOf(i));
//            preparedStatement.setString(2,String.valueOf(i));
//            preparedStatement.setString(3,String.valueOf(i));
//            preparedStatement.setString(4,String.valueOf(i));
//            preparedStatement.execute();
//            preparedStatement.close();
//        }
//        connection.close();
//    }

    public static void main(String[] args) throws IOException {
        try(FileOutputStream fileOutputStream = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\primaryKey.txt")){
            for (Integer i = 10000; i <= 99999 ;i++){
                fileOutputStream.write(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

//    public static void main(String[] args) throws IOException {
//        List<String> nullValueList13 = new ArrayList<>();
//        List<String> nullValueList12 = new ArrayList<>();
//        try(FileInputStream fileInputStream = new FileInputStream("D:\\WeChat\\weChatFile\\WeChat Files\\wxid_9dspycfr46cp22\\FileStorage\\File\\2023-08\\nullValueFile.txt")){
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
//            String key;
//            while((key = bufferedReader.readLine())!=null){
//                nullValueList13.add(key);
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try(FileInputStream fileInputStream = new FileInputStream("D:\\WeChat\\weChatFile\\WeChat Files\\wxid_9dspycfr46cp22\\FileStorage\\File\\2023-08\\空值——1.2\\00\\nullValue.txt")){
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
//            String key;
//            while((key = bufferedReader.readLine())!=null){
//                nullValueList12.add(key);
//            }
//        }
//        nullValueList13.removeAll(nullValueList12);
//        nullValueList13.forEach(key->{
//            System.out.println(key);
//        });
//    }
}
