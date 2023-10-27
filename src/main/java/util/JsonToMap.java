package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Convert;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@Slf4j
public class JsonToMap {
    public Convert jsonToMap(File file){
        Convert convert = null;
        try(FileInputStream fileInputStream = new FileInputStream(file)){
            convert = new ObjectMapper().readValue(fileInputStream, Convert.class);
        } catch (IOException e) {
            log.error("文件转换对象失败",e);
        }
        return convert;
    }
}
