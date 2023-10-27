package Operation;

import entity.Convert;
import itec.ldap.*;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
public class NewWrite extends Thread{
    private Convert convert;
    private ArrayBlockingQueue<LDAPEntry> arrayBlockingQueue;
    private Integer shardingValue;
    private Integer count = 0;

    public NewWrite(Convert convert,ArrayBlockingQueue arrayBlockingQueue,Integer shardingValue){
        this.convert = convert;
        this.arrayBlockingQueue = arrayBlockingQueue;
        this.shardingValue = shardingValue;
    }

    public void writeData() {
        String targetNode = convert.getTargetNodeList().get(shardingValue);
        String[] splits = targetNode.split(":");
        String host = splits[0];
        Integer port = new Integer(splits[1]);
        LDAPConnection ld = null;
        try {
            ld = new LDAPConnection();
            ld.setShardingFlag(false);
            LDAPConstraints ldapconstraints = new LDAPConstraints();
            ldapconstraints.setTimeLimit(convert.getTimeLimit());  //2000毫秒
            ld.setConstraints(ldapconstraints);
            ld.connect(host, port);
            String MGR_DN = convert.getTargetDN();
            String MGR_PW = convert.getTargetPasswd();
            ld.authenticate(MGR_DN, MGR_PW);
        } catch (LDAPException e) {
            log.error("建立连接失败", e);
        }
        if (ld != null) {
            try {
                while (true) {
                    LDAPEntry ldapEntry;
                    Object object = arrayBlockingQueue.take();
                    count++;
                    if (count == 10000){
                        this.sleep(convert.getSleepTime());
                        count = 0;
                    }
                    if (object == Read.POISON_PILL){
                        log.info("分片" + shardingValue + "导数完成");
                        break;
                    }else{
                        ldapEntry = (LDAPEntry) object;
                    }
                    try {
                        ld.delete(ldapEntry.getDN());
                    } catch (LDAPException e) {
                        if (e.getLDAPResultCode() == LDAPException.NO_SUCH_OBJECT) {
                            log.info("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "删除失败，条目不存在");
                        } else {
                            log.error("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "删除失败，其他原因", e);
                        }
                    }
                    try {
                        ld.add(ldapEntry);
                        log.info("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "写成功");
                    } catch (LDAPException e) {
                        log.error("条目:" + ldapEntry.getDN() + ",ip:" + host + ",port=" + port + "写失败", e);
                    }
                }
            } catch (InterruptedException e) {
                log.error("中断失败",e);
            }
        }
    }
    @Override
    public void run(){
        writeData();
    }
}
