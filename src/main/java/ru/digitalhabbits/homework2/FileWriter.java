package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    private Exchanger<List<Pair<String, Integer>>> exchanger;
    private File file;

    public FileWriter(Exchanger<List<Pair<String, Integer>>> exchanger, File file) {
        this.exchanger = exchanger;
        this.file = file;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        try (java.io.FileWriter writer = new java.io.FileWriter(file, true)) {
            while(!Thread.currentThread().isInterrupted()) {

                List<Pair<String, Integer>> list = exchanger.exchange(null);
                if(list == null) {
                    continue;
                }
                StringBuilder builder = new StringBuilder();
                for(Pair<String, Integer> pair : list) {
                    builder.append(pair.getKey())
                            .append(" ")
                            .append(pair.getValue())
                            .append("\n");
                }
                writer.write(builder.toString());
            }
        } catch (IOException e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        } catch (InterruptedException e) {
            logger.info("Writer thread is stopped");
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
