package com.zleying.common;

import java.io.*;
import java.util.Properties;

/**
 * Created by lyden on 2017/7/30.
 */
public final class PropertiesUtil {

    public PropertiesUtil() {

    }

    public  static Properties getProperty(String pathName){
        Properties properties=null;
        BufferedInputStream bufferedInputStream=null;
        try {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(new File(pathName)));
            properties = new Properties();
            properties.load(bufferedInputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
        return properties;
    }

}
