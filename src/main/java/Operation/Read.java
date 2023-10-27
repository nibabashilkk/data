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
public class Read {
    private DBUtil dbUtil;
    private Convert convert;
    private Vector<String> nullValueVector = new Vector<>();
    private Map<Integer, ArrayBlockingQueue<Object>> arrayBlockingQueueMap = new HashMap<>();
    public static final Object POISON_PILL = new Object();

    public Read(DBUtil dbUtil, Convert convert){
        this.dbUtil = dbUtil;
        this.convert = convert;
        for (int i =0; i < convert.getTargetShardingCount(); i ++){
            ArrayBlockingQueue<Object> ldapEntryArrayBlockingQueue = new ArrayBlockingQueue(10000);
            this.arrayBlockingQueueMap.put(i,ldapEntryArrayBlockingQueue);
        }
    }

    public Map<Integer,List<String>> getShardingData(File file){
        Map<Integer,List<String>> shardingData = new HashMap<>();
        try(FileInputStream fileInputStream = new FileInputStream(file)) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
            String primaryKey = null;
            while((primaryKey = bufferedReader.readLine()) != null){
                Optional<Integer> shardingNumber = CalcSharding.calcShardingNum(primaryKey,convert.getTargetShardingCount());
                List<String> list = null;
                if(shardingData.containsKey(shardingNumber.get())){
                    list = shardingData.get(shardingNumber.get());
                }else {
                    list = new ArrayList<>();
                }
                list.add(primaryKey);
                shardingData.put(shardingNumber.get(),list);
            }
        }catch (FileNotFoundException e){
            log.error("文件不存在",e);
        }catch (IOException e){
            log.error("文件打开失败",e);
        }
        return shardingData;
    }

    public void getEntry(List<String> primaryKeyList, Integer shardingValue, CountDownLatch countDownLatch){
        new NewWrite(convert, arrayBlockingQueueMap.get(shardingValue), shardingValue).start();
        int count = 0;
        List<String> list = new ArrayList<>();
        try {
            Connection connection = dbUtil.getConn();
            for (int i = 0; i < primaryKeyList.size(); i++) {
                count++;
                list.add(primaryKeyList.get(i));
                if (count == 1000) {
                    search(list, connection, count, shardingValue);
                    list.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                search(list, connection, count, shardingValue);
            }
            arrayBlockingQueueMap.get(shardingValue).put(POISON_PILL);
            connection.close();
        } catch (SQLException e) {
            log.error("关闭8s数据库连接失败",e);
        } catch (InterruptedException e) {
            log.error("放置毒丸失败",e);
        } finally {
            countDownLatch.countDown();
        }
    }

    public void search(List<String> primaryKeyList, Connection connection, Integer count, Integer shardingValue){
        Map<String, String> attributeMap = convert.getAttribute();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try{
            String sql = "select * from "+convert.getSourceTable()+" where "+convert.getPrimaryKey()+" in (" + generatePlaceholder(count) + ")";
            preparedStatement = connection.prepareStatement(sql);
            for (int j = 0; j < primaryKeyList.size(); j++){
                preparedStatement.setString(j+1,primaryKeyList.get(j));
            }
            resultSet = preparedStatement.executeQuery();
            List<String> resultKeyList = new ArrayList<>();
            while (resultSet.next()) {
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
                }
                if(!ldapAttributeSet.getAttribute(convert.getNullValueAttribute()).getByteValues().hasMoreElements()){
                    String[] s = ldapAttributeSet.getAttribute("sn").getStringValueArray();
                    nullValueVector.add(s[0]);
                } else {
                    LDAPEntry ldapEntry = new LDAPEntry(dn, ldapAttributeSet);
                    arrayBlockingQueueMap.get(shardingValue).put(ldapEntry);
                }
            }
            if(primaryKeyList.size() != resultKeyList.size()){
                primaryKeyList.removeAll(resultKeyList);
                for (String primaryKey : primaryKeyList){
                    log.error("sn="+primaryKey+"查询不存在");
                }
            }
        } catch (SQLException  e) {
            log.error("查询8s错误",e);
        } catch (InterruptedException e) {
            log.error("中断错误",e);
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
}
