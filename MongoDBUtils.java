package util;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.connection.Server;
import jdk.internal.dynalink.MonomorphicCallSite;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class MongoDBUtils {

    /**
     * Connect with MongoDB.
     * Create MongoClient with db connection url and port number
     */
    public MongoClient getClient(String dbConnectionUrl){
            return  new MongoClient(dbConnectionUrl, 27017);
    }

    /**
     * Get Mongo Document collection
     */
    public MongoCollection<Document> getCollection(MongoClient client, String dbName, String collectionName){
        return client.getDatabase(dbName).getCollection(collectionName);
    }

    /**
     * Get Mongo DB table data
     */
    @SuppressWarnings({"resource", "deprecation"})
    public static DBCollection getMongoDB(String dbUrl, String dbName, String collectionName){

        DB db = null;
        String monopass = "123456789";
        boolean isAtlas = true;

        if(isAtlas){
            final MongoClientOptions.Builder builder = new MongoClientOptions.Builder().sslEnabled(true)
                    .sslInvalidHostNameAllowed(true);
            builder.maxConnectionIdleTime(200000);

            final MongoClientOptions options = builder.build();
            final char[] password = monopass.toCharArray();
            String dbConnName = "mongoConnDB";
            final MongoCredential credential = MongoCredential.createCredential(dbConnName, "admin", password);
            final List<ServerAddress> list = new LinkedList<ServerAddress>();
            final String[] urls = dbUrl.split("#");
            for(final String url: urls){
                list.add(new ServerAddress(url,27017));
            }

            final MongoClient mongoClient = new MongoClient(list, Arrays.asList(credential),options);
            db = mongoClient.getDB(dbName);
            final DBCollection table = db.getCollection(collectionName);
            return table;
        }else {
            final MongoClient mongoClient = new MongoClient(dbUrl, 27017);
            // get access to required db by giving db name
            db = mongoClient.getDB(dbName);
            // get db table raw table
            final DBCollection table = db.getCollection(collectionName);
            return table;
        }
    }

    /**
     * Get Mongo Document using response id
     */
    public String getMongoDocumentFromResponseId(String responseId, String dbUrl, String dbName, String collectionName){
        String document = "";
        try{
            final BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("_id", new ObjectId(responseId));
            final DBObject dobj = getMongoDB(dbUrl, dbName, collectionName).findOne(searchQuery);
            document = dobj.toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return document;
    }

    /**
     * Get Mongo Document using execution id
     */
    public String getMongoDocumentFromExecutionId(String responseId, String dbUrl, String dbName, String collectionName){
        List<DBObject> document = null;
        try{
            final BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("_id", new ObjectId(responseId));
            final DBCursor dbCursor = getMongoDB(dbUrl, dbName, collectionName).find(searchQuery);
            document = dbCursor.toArray();
        }catch (Exception e){
            e.printStackTrace();
        }
        return document.toString();
    }

    /**
     * Get Mongo Document count
     */
    public Integer getRecordCount(String dbUrl, String dbName, String collectionName){
        final Integer count = getMongoDB(dbUrl,dbName,collectionName).find().count();
        return count;
    }

    /**
     * Get Mongo Document count with search query
     */
    public int getRecordCountWithSearchQuery(String dbUrl, String dbName, String collectionName){
        final BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("status", "active");
        final int count = getMongoDB(dbUrl,dbName,collectionName).find(searchQuery).count();
        return count;
    }

    /**
     * Get Mongo Document count with search query passed as fieldName and value
     */
    public int getRecordCountWithFieldNameAsQuery(String dbUrl, String dbName, String collectionName, String fieldName, String value){
        final BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put(fieldName, value);
        final int count = getMongoDB(dbUrl,dbName,collectionName).find(searchQuery).count();
        return count;
    }

}
