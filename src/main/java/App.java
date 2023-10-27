import Operation.Read;
import entity.Convert;
import lombok.extern.slf4j.Slf4j;
import util.DBUtil;
import util.JsonToMap;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class App {
    public static void main(String[] args) {
        JsonToMap jsonToMap = new JsonToMap();
        log.info("开始读取配置文件");
        File configFile = new File("C:\\Users\\Administrator\\Desktop\\config.txt");
        File primaryKeyFile = new File("C:\\Users\\Administrator\\Desktop\\primaryKey.txt");
        Convert convert = jsonToMap.jsonToMap(configFile);
        DBUtil dbUtil = new DBUtil(convert);
        Read read = new Read(dbUtil, convert);
        Map<Integer,List<String>> primaryKeyListMap = read.getShardingData(primaryKeyFile);
        CountDownLatch countDownLatch = new CountDownLatch(primaryKeyListMap.keySet().size());
        for (Integer shardingValue : primaryKeyListMap.keySet()){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    read.getEntry(primaryKeyListMap.get(shardingValue), shardingValue, countDownLatch);
                }
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("主线程等待被中断",e);
        }
        Vector<String> nullValueVector = read.getNullValueVector();
        if(!nullValueVector.isEmpty()){
            try(FileOutputStream fileOutputStream = new FileOutputStream("nullValueFile.txt",true)){
                nullValueVector.forEach(sn->{
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
//    public static void main(String[] args){
//        JsonToMap jsonToMap = new JsonToMap();
//        ThreadPool threadPool = new ThreadPool();
//        ThreadPoolExecutor threadPoolExecutor = threadPool.init();
//        log.info("开始读取配置文件");
//        File configFile = new File("config.txt");
//        Convert convert = jsonToMap.jsonToMap(configFile);
//        NewRead read = new NewRead(convert);
//        log.info("配置文件读取完成");
//        log.info("开始读取主键文件");
//        DBUtil dbUtil = new DBUtil(convert);
//        CountDownLatch countDownLatch = new CountDownLatch(convert.getPrimaryKeyList().size());
//        for (String fileName : convert.getPrimaryKeyList()){
//            threadPoolExecutor.submit(new Runnable() {
//                @Override
//                public void run() {
//                    read.readData(new File(fileName),dbUtil,convert,countDownLatch);
//                }
//            });
//        }
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            log.error("主线程等待被中断",e);
//        }
//        Map<Integer, ArrayBlockingQueue<Object>> arrayBlockingQueueMap = read.getArrayBlockingQueueMap();
//        try {
//            for (Integer key : arrayBlockingQueueMap.keySet()){
//                arrayBlockingQueueMap.get(key).put(NewRead.POISON_PILL);
//            }
//        } catch (InterruptedException e) {
//            log.error("放毒丸被中断",e);
//        }
//        Vector<String> nullValueVector = read.getNullValueVector();
//        if(!nullValueVector.isEmpty()){
//            try(FileOutputStream fileOutputStream = new FileOutputStream("nullValueFile.txt",true)){
//                nullValueVector.forEach(sn->{
//                    try {
//                        fileOutputStream.write(sn.getBytes(StandardCharsets.UTF_8));
//                        fileOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
//                    } catch (IOException e) {
//                        log.error("写入失败",e);
//                    }
//                });
//            } catch (FileNotFoundException e) {
//                log.error("文件不存在",e);
//            } catch (IOException e) {
//                log.error("打开文件失败",e);
//            }
//        }
//        threadPoolExecutor.shutdown();
//    }
}
