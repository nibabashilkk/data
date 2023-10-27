package Operation;

import entity.Convert;
import itec.ldap.LDAPAttribute;
import itec.ldap.LDAPAttributeSet;
import itec.ldap.LDAPEntry;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import util.CalcSharding;
import util.DBUtil;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Data
public class NewRead {
    private Map<Integer, ArrayBlockingQueue<Object>> arrayBlockingQueueMap = new HashMap<>();
    private Vector<String> nullValueVector = new Vector<>();
    public static final Object POISON_PILL = new Object();

    public NewRead(Convert convert){
        for (int i =0; i < convert.getTargetShardingCount(); i ++){
            ArrayBlockingQueue<Object> ldapEntryArrayBlockingQueue = new ArrayBlockingQueue(10000);
            this.arrayBlockingQueueMap.put(i,ldapEntryArrayBlockingQueue);
            new NewWrite(convert, ldapEntryArrayBlockingQueue, i).start();
        }
    }
    public void readData(File file, DBUtil dbUtil, Convert convert, CountDownLatch countDownLatch){
        try(FileInputStream fileInputStream = new FileInputStream(file)) {
            Connection connection = dbUtil.getConn();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
            String primaryKey = null;
            Integer index = 0;
            List<String> primaryKeyList = new ArrayList<>();
            while((primaryKey = bufferedReader.readLine()) != null){
                primaryKeyList.add(primaryKey);
                index += 1;
                if(index == 1000){
                    dataDistribution(convert, index,connection,primaryKeyList);
                    primaryKeyList.clear();
                    index = 0;
                }
            }
            if (index != 0){
                dataDistribution(convert,index,connection,primaryKeyList);
            }
            connection.close();
        } catch (FileNotFoundException e) {
            log.error("文件不存在",e);
        } catch (IOException e) {
            log.error("文件打开失败",e);
        } catch (SQLException e) {
            log.error("数据库连接关闭失败",e);
        }finally {
            countDownLatch.countDown();
        }
    }

    public String generatePlaceholder(Integer count){
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < count; i++){
            if (i > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append("?");
        }
        return stringBuilder.toString();
    }
    public void dataDistribution(Convert convert, Integer index, Connection connection, List<String> primaryKeyList){
        Map<String, String> attributeMap = convert.getAttribute();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String sql = "select * from "+convert.getSourceTable()+" where "+convert.getPrimaryKey()+" in (" + generatePlaceholder(index) + ")";
            preparedStatement = connection.prepareStatement(sql);
            for (int j = 0; j < primaryKeyList.size(); j++){
                preparedStatement.setString(j+1,primaryKeyList.get(j));
            }
            resultSet = preparedStatement.executeQuery();
            List<String> resultKeyList = new ArrayList<>();
            while (resultSet.next()) {
                Integer shardingValue = null;
                LDAPAttributeSet ldapAttributeSet = new LDAPAttributeSet();
                LDAPAttribute ldapAttribute = null;
                String dn = null;
                for (String key : attributeMap.keySet()) {
                    String xdmAttribute = attributeMap.get(key);
                    ldapAttribute = new LDAPAttribute(xdmAttribute);
                    String value = resultSet.getString(key);
                    //先不管属性null值
                    ldapAttribute.addValue(value);
                    ldapAttributeSet.add(ldapAttribute);
                    if (xdmAttribute.equals("sn")) {
                        dn = "sn=" + value + ",dc=" + convert.getDc();
                        resultKeyList.add(value);
                    }
                    if(xdmAttribute.equals(convert.getTargetShardingAttribute())){
                        shardingValue = CalcSharding.calcShardingNum(value, convert.getTargetShardingCount()).get();
                    }
                }
                if(!ldapAttributeSet.getAttribute("tzmb-zz;binary").getByteValues().hasMoreElements()){
                    String[] s = ldapAttributeSet.getAttribute("sn").getStringValueArray();
                    nullValueVector.add(s[0]);
                } else {
                    LDAPEntry ldapEntry = new LDAPEntry(dn, ldapAttributeSet);
                    if (shardingValue != null){
                        arrayBlockingQueueMap.get(shardingValue).put(ldapEntry);
                    }
                }
            }
            if(primaryKeyList.size() != resultKeyList.size()){
                primaryKeyList.removeAll(resultKeyList);
                for (String primaryKey : primaryKeyList){
                    log.error("sn="+primaryKey+"查询不存在");
                }
            }
        } catch (SQLException e) {
            log.error("读取数据失败", e);
        } catch (InterruptedException e) {
            log.error("中断异常",e);
        } finally {
            if(resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    log.error("resultSet释放失败",e);
                }
            }
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    log.error("preparedStatement释放失败",e);
                }
            }
        }
    }
}
