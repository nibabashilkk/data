package Operation;

import entity.Convert;
import itec.ldap.*;
import lombok.extern.slf4j.Slf4j;
import util.DBUtil;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class Write extends Thread{
    private DBUtil dbUtil;
    private Integer shardingNumber;
    private Convert convert;
    private List<String> primaryList;
    private List<String> nullValueList = new ArrayList<>();

    public Write(List<String> primaryList,Integer shardingNumber,Convert convert,DBUtil dbUtil){
        this.primaryList = primaryList;
        this.convert = convert;
        this.dbUtil = dbUtil;
        this.shardingNumber = shardingNumber;
    }

    public void writeData(List<String> primaryList,Integer shardingNumber, Convert convert,DBUtil dbUtil){
        String targetNode = convert.getTargetNodeList().get(shardingNumber);
        String[] splits = targetNode.split(":");
        String host = splits[0];
        Integer port = new Integer(splits[1]);
        LDAPConnection ld = null;
        try {
            ld = new LDAPConnection();
            ld.setShardingFlag(false);
            ld.setConnectTimeout(3);
            ld.setConnectMilliTimeout(3000);
            LDAPConstraints ldapconstraints = new LDAPConstraints();
            ldapconstraints.setTimeLimit(20000);  //2000毫秒
            ld.setConstraints(ldapconstraints);
            ld.connect(host, port);
            String MGR_DN = convert.getTargetDN();
            String MGR_PW = convert.getTargetPasswd();
            ld.authenticate(MGR_DN, MGR_PW);
        }catch (LDAPException e) {
            log.error("建立连接失败",e);
        }
        LDAPConnection finalLd = ld;
        if(ld != null) {
            primaryList.forEach(primary -> {
                boolean isNull = false;
                Connection connection = dbUtil.getConn();
                Map<String, String> attributeMap = convert.getAttribute();
                Vector<LDAPEntry> entries = new Vector<>();
                PreparedStatement preparedStatement = null;
                ResultSet resultSet = null;
                try {
                    String sql = "select * from "+convert.getSourceTable()+" where m_code = ?";
                    preparedStatement = connection.prepareStatement(sql);
                    preparedStatement.setString(1, primary);
                    resultSet = preparedStatement.executeQuery();
                    while (resultSet.next()) {
                        LDAPAttributeSet ldapAttributeSet = new LDAPAttributeSet();
                        LDAPAttribute ldapAttribute = null;
                        String dn = null;
                        for (String key : attributeMap.keySet()) {
                            String xdmAttribute = attributeMap.get(key);
                            ldapAttribute = new LDAPAttribute(xdmAttribute);
                            String value = resultSet.getString(key);
                            //先不管属性null值
//                            if(value == null){
//                                ldapAttribute.addValue("");
//                            }else{
                                ldapAttribute.addValue(value);
//                            }
                            ldapAttributeSet.add(ldapAttribute);
                            if (xdmAttribute.equals("sn")) {
                                dn = "sn=" + value + ",dc=" + convert.getDc();
                            }
                        }
                        if(!ldapAttributeSet.getAttribute("tzmb-zz;binary").getByteValues().hasMoreElements()){
                            String[] s = ldapAttributeSet.getAttribute("sn").getStringValueArray();
                            nullValueList.add(s[0]);
                            isNull = true;
                        }
                        LDAPEntry ldapEntry = new LDAPEntry(dn, ldapAttributeSet);
                        entries.add(ldapEntry);
                    }
                } catch (SQLException e) {
                    log.error("读取数据失败", e);
                }finally {
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
                    if(connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            log.error("connection释放失败",e);
                        }
                    }
                }
                if(!isNull) {
                    for (LDAPEntry ldapEntry : entries) {
                        try {
                            finalLd.delete(ldapEntry.getDN());
                        } catch (LDAPException e) {
                            if (e.getLDAPResultCode() == LDAPException.NO_SUCH_OBJECT) {
                                log.info("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "删除失败，条目不存在");
                            } else {
                                log.error("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "删除失败，其他原因", e);
                            }
                        }
                        try {
                            finalLd.add(ldapEntry);
                            log.info("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "写成功");
                        } catch (LDAPException e) {
                            log.error("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "写失败", e);
                        }
                    }
                }
            });
        }
        if (ld != null&&ld.isConnected()){
            try{
                ld.disconnect();
            } catch (LDAPException e) {
                log.error("ld释放连接失败",e);
            }
        }
        if(!nullValueList.isEmpty()){
            try(FileOutputStream fileOutputStream = new FileOutputStream(String.valueOf(shardingNumber),true)){
                nullValueList.forEach(sn->{
                    try {
                        fileOutputStream.write(sn.getBytes(StandardCharsets.UTF_8));
                        fileOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        log.error("写入失败",e);
                    }
                });
            } catch (FileNotFoundException e) {
                log.error("文件不存在",e);
            } catch (IOException e) {
                log.error("打开文件失败",e);
            }
        }
    }
    @Override
    public void run(){
        writeData(primaryList,shardingNumber,convert,dbUtil);
    }
}
