package entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Convert {
    private String jdbc;
    private String sourceUser;
    private String sourcePasswd;
    private String sourceTable;
    private String targetDN;
    private String targetPasswd;
    private String targetShardingAttribute;
    private Integer targetShardingCount;
    private List<String> targetNodeList;
    private Integer timeLimit;
    private String dc;
    private String primaryKey;
    private String nullValueAttribute;
    private Integer sleepTime;
    private Map<String,String> attribute;
}
