package reader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;

public class LogReader {

    private List<String> lineQueue = new LinkedList<>();
    private Map<String, JSONObject> lookupCache = new ConcurrentHashMap<>();
    private List<Processor> processors = new LinkedList<>();

    private Logger logger;

    private String databaseName;

    private volatile boolean threadsExit = false;

    public LogReader() throws Exception {
        getLogger().log(Level.INFO, "created LogReader");
    }

    synchronized Logger getLogger() {
        Logger logger = Logger.getAnonymousLogger();
        logger.setLevel(Level.ALL);
        Logger root = Logger.getLogger("");
        root.setLevel(Level.ALL);
        for (Handler h : root.getHandlers()) {
            h.setLevel(Level.ALL);
        }
        return logger;
    }

    /**
     * debugging help.
     */
    public void reportLookupCache() {
        synchronized (lookupCache) {
            for (Map.Entry<String, JSONObject> ce : lookupCache.entrySet()) {
                getLogger().log(Level.INFO, "ce:" + ce);
            }
        }
    }

    public void startProcessors() throws Exception {
        int processorsCount = Runtime.getRuntime().availableProcessors() - 1;
        for (int i = 0; i < processorsCount; i++) {
            Processor processor = new Processor();
            processor.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    getLogger().log(Level.SEVERE, "Processor failed with: ", e);
                }
            });
            processors.add(processor);
            processor.start();
        }
        getLogger().log(Level.INFO, "Started " + processors.size() + " processors.");
    }

    public void stopProcessors() {
        while (true) {
            if (getLineQueueSize() == 0 && getLookupCacheSize() == 0) {
                threadsExit = true;
                getLogger().log(Level.INFO, "stopped all processors");
                break;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {}
            }
        }
    }

    public void initializeDatabase(String fileName) throws Exception {
        this.databaseName = fileName;
        Connection c = createConnection();
        Statement stmt = c.createStatement();
        stmt.execute("create table EVENTS (id varchar(100) not null, duration int, type varchar(50), host varchar(50), alert int)");
        stmt.close();
        c.close();
        getLogger().log(Level.INFO, "Created table EVENTS");
    }

    public void loadFileAndProcess(File file) throws Exception {
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = fileReader.readLine()) != null) {
            synchronized (lineQueue) {
                lineQueue.add(line);
            }
        }
    }

    private Connection createConnection() throws Exception {
        return DriverManager.getConnection("jdbc:hsqldb:file:" + databaseName, "SA", "");
    }

    private int getLineQueueSize() {
        synchronized (lineQueue) {
            return lineQueue.size();
        }
    }

    private int getLookupCacheSize() {
        synchronized (lookupCache) {
            return lookupCache.size();
        }
    }

    class Processor extends Thread {
        private JSONParser parser;
        private Connection connection;

        Processor() throws Exception {
            parser = new JSONParser();
            connection = createConnection();
        }

        public void run() {

            while (!threadsExit || getLineQueueSize() != 0 || getLookupCacheSize() != 0) {

                try {
                    String line = null;
                    boolean found = false;
                    JSONObject pairObject = null;
                    JSONObject object = null;

                    synchronized (lineQueue) {
                        if (lineQueue.size() > 0) {
                            line = lineQueue.remove(0);
                        }
                    }

                    if (line != null) {
                        // parse and write to database
                        object = (JSONObject) parser.parse(line);
                        String id = (String) object.get("id");

                        synchronized (lookupCache) {
                            pairObject = lookupCache.get(id);
                            if (pairObject != null) {
                                // we have a pair, store in the database
                                lookupCache.remove(id, pairObject);
                                found = true;
                            } else {
                                lookupCache.put(id, object);
                            }
                        }
                        if (found) {
                            storeInTheDatabase(pairObject, object);
                        }
                    } else {
                        Thread.sleep(500);
                    }
                } catch (InterruptedException e) {
                    getLogger().log(Level.SEVERE, " interrupted ");
                } catch (Exception e) {
                    getLogger().log(Level.SEVERE, "lookup or processing failed:", e);
                }
            }
        }

        void storeInTheDatabase(JSONObject first, JSONObject second) throws Exception {
            PreparedStatement ps = null;
            try {
                getLogger().log(Level.INFO, "storing in db.");
                ps = connection.prepareCall("insert into EVENTS (id, duration, type, host, alert) values (?, ?, ?, ?, ?)");
                ps.setString(1, (String)first.get("id"));
                long duration = (Long)first.get("timestamp") - (Long)second.get("timestamp");
                ps.setLong(2, Math.abs(duration));
                ps.setString(3, (String)first.get("type"));
                ps.setString(4, (String)first.get("host"));
                ps.setInt(5, duration > 4 ? 1 : 0);
                ps.execute();
            } catch (SQLException e) {
                getLogger().log(Level.SEVERE, "Storing in DB failed with: ", e);
            }
            finally {
                ps.close();
            }
        }

    }

    public static void main(String[] args) {
        try {

            LogReader logReader = new LogReader();
            logReader.initializeDatabase("db/db-" + System.currentTimeMillis());
            logReader.startProcessors();
            logReader.loadFileAndProcess(new File(args[0] + "logfile.txt"));
            logReader.stopProcessors();
            logReader.reportLookupCache();
        } catch (Exception e) {
            Logger.getGlobal().log(Level.SEVERE, "Log reader failed", e);
        }
    }
}
