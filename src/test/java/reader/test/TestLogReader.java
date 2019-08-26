package reader.test;

import org.junit.Test;
import reader.LogReader;

import java.io.File;

public class TestLogReader {

    @Test
    public void testLoadFileAndProcess() throws Exception {
        LogReader logReader = new LogReader();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        logReader.initializeDatabase("output.db");
        logReader.startProcessors();
        logReader.loadFileAndProcess(new File(classLoader.getResource("logfile.txt").toURI()));
    }
}
