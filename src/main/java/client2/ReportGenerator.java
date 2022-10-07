package client2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;


public class ReportGenerator {
    public Map<String, List<Report>> reportMap;
    public List<Report> reportList;

    public ReportGenerator(Map<String, List<Report>> reportMap) {
        this.reportMap = reportMap;
        this.reportList = new ArrayList<>();
        for (List<Report> reports : reportMap.values()) {
            reportList.addAll(reports);
        }
    }

    public void generateReport() {
        List<Integer> sortedLatency = reportList.stream().map(Report::getLatency).collect(Collectors.toList());
        Collections.sort(sortedLatency);
        int lenOfReport = sortedLatency.size() - 1;

        int minLatency = sortedLatency.get(0);
        int maxLatency = sortedLatency.get(lenOfReport);
        int medianLatency = sortedLatency.get(lenOfReport / 2);
        double sumOfLatency = sortedLatency.stream().reduce(0, Integer::sum);
        double meanLatency = sumOfLatency / (double) lenOfReport;
        int ninetyNinePercentile = percentile(sortedLatency, 99);

        System.out.printf("Min Latency: %d%n", minLatency);
        System.out.printf("Max Latency: %d%n", maxLatency);
        System.out.printf("Average Latency: %f.2%n", meanLatency);
        System.out.printf("Median Latency: %d%n", medianLatency);
        System.out.printf("99th percentile Latency: %d%n", ninetyNinePercentile);
    }

    private int percentile(List<Integer> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }

    public void writeToCSV() {
        String filePath = String.format("output/Latency Report - %S.csv", new Date(System.currentTimeMillis()));

        List<List<String>> reports = reportList.stream().map(Report::getList).collect(Collectors.toList());
        List<String> headers = List.of("Start Time", "End Time", "Latency", "Request Type", "Response Status Code");
        try {
            CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(filePath), CSVFormat.EXCEL);
            csvPrinter.printRecord(headers);
            csvPrinter.printRecords(reports);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
