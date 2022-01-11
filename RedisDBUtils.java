package util;


import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

public class RedisDBUtils {

    public Jedis connectRedisDB(String dburl){
        final Jedis jedis = new Jedis(dburl, 6379, 200000);
        jedis.connect();
        return jedis;
    }


    public Integer getUniverseEstimateCount(String url, String tname, String  key){
        final int uCount = Integer.parseInt(connectRedisDB(url).get(tname + "_"+ key));
        return uCount;
    }

    public List<String > getDBListValues(String dburl, String listName){
        final List<String> db = connectRedisDB(dburl).lrange(listName, 0, 100000);
        return db;
    }

    String redisDBurl = "";
    public String  checkKeyData(String entityKey, String entityItemKey){
        String value = "";
        Jedis jedis = connectRedisDB(redisDBurl);
        jedis.select(0);
        List<String> dbList = jedis.lrange(entityKey, 0, -1);

        for(String s: dbList){
            if(s.contains(entityItemKey)){
                value = s;
                break;
            }
        }
        return value;
    }

    public List<String>  getListOfRecords (String entityKey){

        Jedis jedis = connectRedisDB(redisDBurl);
        jedis.select(0);
        List<String> dbList = jedis.lrange(entityKey, 0, -1);
        return dbList;
    }

    public String checkHashKeyData(String entityKey, String entityItemId){
        String value = "";
        Jedis jedis = connectRedisDB(redisDBurl);
        jedis.select(0);
        Map<String, String> dbMapData = jedis.hgetAll(entityKey);
        for(Map.Entry<String, String > item: dbMapData.entrySet()){
            if(item.getKey().contains(entityItemId)){
                value = item.getValue();
                break;
            }
        }
        return value;
    }
}
