package fi.mamk.osa.microservices;

import java.io.File;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import com.belvain.soswe.workflow.Microservice;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import net.xeoh.plugins.base.annotations.Capabilities;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
public class SaveToMongo extends Microservice {
    
    private static final String MONGO_DEFAULTHOST = "127.0.0.1";
    private static final int MONGO_DEFAULTPORT = 27017;
    private static final String MONGO_DEFAULTDB = "db";
    private static final String MONGO_DEFAULTCOLLECTION = "metadatafiles";
    
    private MongoClient mongo;
    private DB db;
    private DBCollection table;
    
    private String host;
    private int port;
    private String defaultDB;
    private String defaultCollection;
    
    public SaveToMongo() {
        
        this.host = MONGO_DEFAULTHOST;
        this.port = MONGO_DEFAULTPORT;
        this.defaultDB = MONGO_DEFAULTDB;
        this.defaultCollection = MONGO_DEFAULTCOLLECTION;

        try {
            this.mongo = new MongoClient(host, port);
            this.db = mongo.getDB(defaultDB);
            this.table = db.getCollection(defaultCollection);
            
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Capabilities
    public String[] caps() {
        return new String[] {"name:SaveToMongo"};
    }

    @Override
    public boolean execute(String input, HashMap<String, Object> options)
            throws Exception {
        boolean success = false;
        String state = "error";
        String output = "";
        String dir = "";
        String organization = "";
        String username = "";
        String usermail = "";
        String filename = "";
        HashMap<String, Object> metadata = new HashMap<String, Object>();
        Iterator<Entry<String, Object>> it = null;
        
        if (options != null) {

            if (options.containsKey("ingestdirectory")) {
                dir = options.get("ingestdirectory").toString();
            }
            if (options.containsKey("organization")) {
                organization = options.get("organization").toString();
            }
            if (options.containsKey("username")) {
                username = options.get("username").toString();
            }
            if (options.containsKey("usermail")) {
                usermail = options.get("usermail").toString();
            }
            if (options.containsKey("filename")) {
                filename = options.get("filename").toString();
            }
            if (options.containsKey("metadata")) {
                String[] elements = options.get("metadata").toString().replace("[", "").replace("]", "").split(",");
                for (String md: elements) {
                    String[] keyValue = md.split("=");
                    metadata.put(keyValue[0], keyValue[1]);
                }
            }
        }
        
        try {
        
            BasicDBObject updateQuery = new BasicDBObject("username", organization+":"+usermail);
            DBCursor cursor = table.find(updateQuery, new BasicDBObject(MONGO_DEFAULTCOLLECTION,1).append("_id",false));
            Vector<Object> metadatafiles = new Vector<Object>();
            BasicDBObject command = new BasicDBObject();
            
            LinkedHashMap<String, MetaDataElement> map = new LinkedHashMap<String, MetaDataElement>();
            MetaDataElement fileElem = new MetaDataElement();
            fileElem.setName("filename");
            fileElem.setValue(filename);
            map.put("filename", fileElem);
            
            // title
            MetaDataElement titleElem = new MetaDataElement();
            titleElem.setName("title");
            titleElem.setValue(filename);
            map.put("title", titleElem);
    
            // fileName
            MetaDataElement nameElem = new MetaDataElement();
            nameElem.setName("fileName");
            nameElem.setValue(filename);
            map.put("fileName", nameElem);
            
            // extentsize
            File file = new File(dir+filename);
            if (file.exists()) {
                double bytes = file.length();
                double kilobytes = (bytes / 1024);
                String result = String.format("%.2f", kilobytes)+" KB";
                
                MetaDataElement sizeElem = new MetaDataElement();
                sizeElem.setName("extentsize");
                sizeElem.setValue(result);
                map.put("extentsize", sizeElem);
            }
            
            if (metadata != null) {
                it = metadata.entrySet().iterator();
                
                // read metadata
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry)it.next();
                    MetaDataElement mdElem = new MetaDataElement();
                    mdElem.setName(pairs.getKey().toString());
                    mdElem.setValue(pairs.getValue().toString());
                    map.put(pairs.getKey().toString(), mdElem);
                    output += "Metadata element added: "+pairs.getKey().toString()+" : "+pairs.getValue().toString()+"\n";
                }
            }
            
            Object serializedMap = JSON.parse(new flexjson.JSONSerializer().deepSerialize(map));
            
            // User already has atleast one file added to metadata list
            if (cursor.size() != 0) {
                // Given file is not yet on the list, update db with the file
                if (!cursor.next().get(MONGO_DEFAULTCOLLECTION).toString().contains(filename)) {
                    command.put("$push", new BasicDBObject(MONGO_DEFAULTCOLLECTION, serializedMap));
                    this.table.update(updateQuery, command);
                // Given file is already on the list, update it's metadatas
                } else {
                    command = new BasicDBObject();
                    // Appends filename to filter so query is "get from this user where filename is this"
                    updateQuery.append(MONGO_DEFAULTCOLLECTION+".filename.value", filename);
                    command.put("$set", new BasicDBObject(MONGO_DEFAULTCOLLECTION+".$", serializedMap));
                    
                    this.table.update(updateQuery, command, true, false);
                }
            // User does not have any files added to metadata list
            } else {
                metadatafiles.add(serializedMap);
                command.put("username", organization+":"+usermail);
                command.put(MONGO_DEFAULTCOLLECTION, metadatafiles);
                this.table.insert(command);                
            }
                   
            cursor.close();
            output += "Metadata for "+filename+" saved to mongoDB\n";
            
            state = "completed";
            success = true;
        
        } catch (Exception e) {
            output += "Adding metadata for "+filename+" failed.\n";
        }
        
        super.setState(state);
        super.setOutput(output);
        super.setCompleted(true);
        
        String log = super.getLog().replace("{organization}", organization).replace("{user}", username);
        super.setLog(log);
        log();
        
        return success;
    }

}
