package io.confluent.developer.analysis2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class AnalysisExample {
    public static final String INPUT_TOPIC = "analysis_incoming2";

    final static List<AnalysisTestConsumer> consumers = new ArrayList<>();
    final static List<AnalysisProducer> producers = new ArrayList<>();
    final static int threadCount = 3;
    final static ExecutorService consumerExecutor = Executors.newFixedThreadPool(threadCount);
    final static ExecutorService producerExecutor = Executors.newFixedThreadPool(threadCount);

    private static void createProducerThreads() throws IOException {
        for (int i = 0; i < threadCount; i++) {
            System.out.println("...created AnalysisProducer thread");
            AnalysisProducer analysisProducer = new AnalysisProducer();
            producers.add(analysisProducer);
            producerExecutor.submit(analysisProducer);
        }

    }

    private static void createConsumerThreads() throws IOException {
        for (int i = 0; i < threadCount; i++) {
            System.out.println("...created AnalysisProducer thread");
            AnalysisTestConsumer analysisTestConsumer = new AnalysisTestConsumer();
            consumers.add(analysisTestConsumer);
            consumerExecutor.submit(analysisTestConsumer);
        }

    }

    private static void addShutdownHook() {
        // output will not be seen when running via gradlew
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("=====[shutdown requested]=====");

            for (AnalysisProducer producer: producers) {
                producer.shutdown();
            }
            for (AnalysisTestConsumer consumer: consumers) {
                consumer.shutdown();
            }

            producerExecutor.shutdown();
            try {
                if (producerExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    System.out.println("producer threads terminated");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            consumerExecutor.shutdown();
            try {
                if (consumerExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    System.out.println("consumer threads terminated");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

    }

    public static void main(String[] args)  {
        System.out.println("=====[analysis]=====");
        try {
            addShutdownHook();
            createProducerThreads();
            createConsumerThreads();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}