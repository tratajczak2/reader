package reader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

public class LogReader {

    private List<String> lineQueue = new LinkedList<>();
    private Map<String, JSONObject> lookupCache = new HashMap<>();
    private List<Processor> processors = new LinkedList<>();

    private Logger logger;

    private String databaseName;

    public LogReader() throws Exception {
        logger = Logger.getLogger(LogReader.class.getName());
        Handler handler = new StreamHandler(System.out, new SimpleFormatter());
        handler.setLevel(Level.ALL);
        logger.setLevel(Level.ALL);
        logger.addHandler(handler);
        logger.log(Level.INFO, "created logger");
    }

    public synchronized Logger getLogger() {
        return logger;
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
        for (int i = 0; i < processors.size(); i++) {
            processors.get(i).interrupt();
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

    class Processor extends Thread {
        private boolean interrupted = false;
        private JSONParser parser;
        private Connection connection;

        Processor() throws Exception {
            parser = new JSONParser();
            connection = createConnection();
            this.setDaemon(false);
        }

        public void run() {
            String line = null;
            while (!interrupted || getLineQueueSize() != 0) {


                if (isInterrupted()) {
                    interrupted = true;
                    continue;
                }
                if (!interrupted || getLineQueueSize() != 0) {

                    boolean found = false;
                    JSONObject pairObject = null;
                    JSONObject object = null;

                    synchronized (lineQueue) {
                        if (lineQueue.size() > 0) {
                            line = lineQueue.remove(0);
                        }
                    }
                    if (line != null) {
                        try {
                            // parse and write to database
                            object = (JSONObject) parser.parse(line);
                            String id = (String) object.get("id");

                            synchronized (lookupCache) {
                                pairObject = lookupCache.get(id);
                                if (pairObject != null) {
                                    // we have a pair store in the database
                                    lookupCache.remove(pairObject);
                                    found = true;

                                } else {
                                    lookupCache.put(id, object);
                                }
                            }
                            if (found) {
                                storeInTheDatabase(pairObject, object);
                            }
                        } catch (Exception e) {
                            getLogger().log(Level.SEVERE, "Parsing JSON failed with: " + line, e);
                        }
                    } else {

                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            interrupted = true;
                        }
                    }
                }
            }
        }

        void storeInTheDatabase(JSONObject first, JSONObject second) throws Exception {
            PreparedStatement ps = null;
            try {
                getLogger().log(Level.SEVERE, "storing in db.");
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
        } catch (Exception e) {
            Logger.getGlobal().log(Level.SEVERE, "Log reader failed", e);
        }
    }
}
