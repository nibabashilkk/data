package util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    public ThreadPoolExecutor init(){
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4,16,10, TimeUnit.SECONDS,new ArrayBlockingQueue<>(1000));
        return threadPoolExecutor;
    }
}
