package com.his.reportService.service;

import org.his.core.dto.ReportPayloadDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
public class ReportPrintService {
    @Value("${puppeteer.script.path}")
    private String nodeScriptPath;

    public String generateReportData(ReportPayloadDto reportPayloadDto) throws InterruptedException, IOException {
        // Customize margins and format
        String marginTop = "1cm";
        String marginBottom = "1cm";
        String marginLeft = "1.5cm";
        String marginRight = "1.5cm";
        String format = "A4";


        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MMM-yyyy"));

        String outputPath = "D:/Hospital MS/generatedReport/" +currentDate+"/" ;
        File filePath = new File(outputPath);
        if(!filePath.exists()){
          filePath.mkdirs();
        }


        String filename = "report_" + reportPayloadDto.getUniqueJobName() + ".pdf";
        System.out.println(reportPayloadDto.toString());
        String finalOutputPath = outputPath + filename;
        ProcessBuilder builder = new ProcessBuilder(
                "node",
                nodeScriptPath,
                reportPayloadDto.getWebPageUrl(),
                finalOutputPath,
                marginTop,
                marginBottom,
                marginLeft,
                marginRight,
                format
        );

        builder.redirectErrorStream(true);
        Process process = builder.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            reader.lines().forEach(System.out::println);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("PDF generation failed with exit code " + exitCode);
        }
        return finalOutputPath;
    }

}
