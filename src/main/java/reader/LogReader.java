package reader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

public class LogReader {

    private List<String> lineQueue = Collections.synchronizedList(new LinkedList<>());
    private Map<String, JSONObject> lookupCache = new HashMap<>();
    private List<Processor> processors = new LinkedList<>();

    private Logger logger;

    private String databaseName;

    public LogReader() {
        logger = Logger.getLogger("logger");
        logger.addHandler(new StreamHandler(System.out, new SimpleFormatter()));
        logger.log(Level.INFO, "created logger");
    }

    public void startProcessors() throws Exception {
        int processorsCount = Runtime.getRuntime().availableProcessors() - 1;
        for (int i = 0; i < processorsCount; i++) {
            Processor processor = new Processor();
            processor.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger.log(Level.SEVERE, "Processor failed with: ", e);
                }
            });
            processor.start();
            processors.add(processor);
        }
        logger.log(Level.SEVERE, "Started " + processors.size() + " processors.");
    }

    public void stopProcessors() {
        for (int i = 0; i < processors.size(); i++) {
            processors.get(i).interrupt();
        }
    }

    public void initializeDatabase(String fileName) throws Exception {
        Connection c = createConnection();
        Statement stmt = c.createStatement();
        stmt.execute("create table EVENTS (id varchar(100) not null, duration int, type varchar(50), host varchar(50), alert int)");
        stmt.close();
        c.close();
        logger.log(Level.INFO, "Created table EVENTS");
    }

    public void loadFileAndProcess(File file) throws Exception {
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = fileReader.readLine()) != null) {
            lineQueue.add(line);
        }

    }

    private Connection createConnection() throws Exception {
        return DriverManager.getConnection("jdbc:hsqldb:file:" + databaseName, "SA", "");
    }

    class Processor extends Thread {
        private boolean interrupted = false;
        private JSONParser parser;
        private Connection connection;

        Processor() throws Exception {
            parser = new JSONParser();
            connection = createConnection();
        }

        public void run() {
            while (!interrupted) {
                try {

                    if (isInterrupted()) {
                        interrupted = true;
                        continue;
                    }

                    if (lineQueue.size() > 0) {
                        // parse and write to database
                        JSONObject object = (JSONObject) parser.parse(lineQueue.get(0));
                        String id = (String) object.get("id");
                        synchronized (lookupCache) {
                            JSONObject pairObject = lookupCache.get(id);
                            if (pairObject != null) {
                                // we have a pair store in the database
                                storeInTheDatabase(pairObject, object);
                            } else {
                                lookupCache.put(id, object);
                            }
                        }
                    } else {
                        Thread.currentThread().sleep(500);
                    }

                } catch (Exception e) {
                    logger.log(Level.WARNING, "Parsing JSON failed with: ", e);
                }
            }
        }

        void storeInTheDatabase(JSONObject first, JSONObject second) throws Exception {
            PreparedStatement ps = null;
            try {
                ps = connection.prepareCall("insert into EVENTS (id, duration, type, host, alert) values (?, ?, ?, ?, ?)");
                ps.setString(1, (String)first.get("id"));
                long duration = (Long)first.get("timestamp") - (Long)second.get("timestamp");
                ps.setLong(2, Math.abs(duration));
                ps.setString(3, (String)first.get("type"));
                ps.setString(4, (String)first.get("host"));
                ps.setBoolean(5, duration > 4);
                ps.execute();
            } catch (SQLException e) {
                logger.log(Level.WARNING, "Parsing JSON failed with: ", e);
            }
            finally {
                ps.close();
            }
        }

    }

    static void main(String[] args) {
        try {

            LogReader logReader = new LogReader();
            logReader.initializeDatabase("output.db");
            logReader.startProcessors();
            logReader.loadFileAndProcess(new File(args[0] + "logfile.txt"));
        } catch (Exception e) {
            Logger.getGlobal().log(Level.SEVERE, "Log reader failed");
        }
    }
}
