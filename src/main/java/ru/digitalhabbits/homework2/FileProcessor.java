package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();
    private final Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
    private final List<CompletableFuture<Pair<String, Integer>>> futures = new ArrayList<>();
    private final LineProcessor<Integer> lineProcessor = new LineCounterProcessor();
    private ExecutorService executorService;

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        final File resultFile = new File(resultFileName);
        if(resultFile.exists()) {
            resultFile.delete();
        }
        // TODO: NotImplemented: запускаем FileWriter в отдельном потоке
        Thread threadFileWriter = new Thread(new FileWriter(exchanger, resultFile));
        threadFileWriter.start();

        executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                // TODO: NotImplemented: вычитываем CHUNK_SIZE строк для параллельной обработки
                List<String> stringList = new ArrayList<>(CHUNK_SIZE);
                stringList.add(scanner.nextLine());
                for(int i = 1; i < CHUNK_SIZE; i++) {
                    if(scanner.hasNext()) {
                        stringList.add(scanner.nextLine());
                    }
                }

                // TODO: NotImplemented: обрабатывать строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                submitTasksForExecution(stringList);
                List<Pair<String, Integer>> resultList = getResultsOfThreads();

                // TODO: NotImplemented: добавить обработанные данные в результирующий файл
                exchanger.exchange(resultList);
                futures.clear();
            }
        } catch (IOException exception) {
            logger.error("", exception);
        } catch (InterruptedException | ExecutionException e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }

        // TODO: NotImplemented: остановить поток writerThread
        threadFileWriter.interrupt();
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

    private void submitTasksForExecution(List<String> stringList) {
        for (String line : stringList) {
            CompletableFuture<Pair<String, Integer>> completableFuture = CompletableFuture.supplyAsync(() ->
                    lineProcessor.process(line), executorService);
            futures.add(completableFuture);
        }
    }

    private List<Pair<String, Integer>> getResultsOfThreads() throws ExecutionException, InterruptedException {
        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        List<Pair<String, Integer>> pairList = voidCompletableFuture.thenApply(fut -> futures.stream()
                .map(CompletableFuture::join).collect(Collectors.toList()))
                .thenApply(list -> {
                    List<Pair<String, Integer>> pairs = new ArrayList<>(futures.size());
                    pairs.addAll(list);
                    return pairs;
                })
                .get();
        return pairList;
    }
}
