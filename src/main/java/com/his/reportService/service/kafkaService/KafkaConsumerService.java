package com.his.reportService.service.kafkaService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.his.reportService.service.ReportPrintService;
import org.his.core.dto.PatientMasterDto;
import org.his.core.dto.ReportGenerationPayloadDto;
import org.his.core.dto.ReportLogsDto;
import org.his.core.dto.ReportPayloadDto;
import org.his.core.kafka.KafkaTopics;
import org.his.core.kafka.services.KafkaProducerService;
import org.his.core.utiltiy.CommonUtility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private ReportPrintService reportPrintService;
    Runtime runtime = Runtime.getRuntime();

    @KafkaListener(topics = "#{T(org.his.core.kafka.KafkaTopics).REPORT_GENERATION.getTopicName()}", groupId = "my-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenToMainTopic(String message, Acknowledgment acknowledgment) {
        processMessage(message, acknowledgment, KafkaTopics.REPORT_GENERATION_RETRY.getTopicName());
    }

    @KafkaListener(topics = "#{T(org.his.core.kafka.KafkaTopics).REPORT_GENERATION_RETRY.getTopicName()}", groupId = "my-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenToRetryTopic(String message, Acknowledgment acknowledgment) {
        processMessage(message, acknowledgment, KafkaTopics.REPORT_GENERATION_RETRY.getTopicName());
    }

    private void processMessage(String message, Acknowledgment acknowledgment, String retryTopic) {
        ObjectMapper objectMapper = new ObjectMapper();
        ReportGenerationPayloadDto reportPayloadDto = new ReportGenerationPayloadDto();
        String reportPayloadWithStatus = "";

        try {

            reportPayloadDto = objectMapper.readValue(message, ReportGenerationPayloadDto.class);
            reportPayloadDto.setWebPageUrl("http://localhost:4200/report");
            runtime.gc();
            long memoryUsedBefore = runtime.totalMemory() - runtime.freeMemory();
            long processStartTime = System.nanoTime();
            String reportPath = reportPrintService.generateReportData(reportPayloadDto);
            long processEndTime = System.nanoTime();
            long memoryUsedAfter = runtime.totalMemory() - runtime.freeMemory();

            reportPayloadDto.setTimeTakenToGenerate(getTimeTakenForReportProcess(processEndTime, processStartTime));
            reportPayloadDto.setMemoryUsedUpToGenerate(getTotalMemoryUsedInKb(memoryUsedAfter, memoryUsedBefore));
            reportPayloadDto.setReportGenerationSucceed(true);
            reportPayloadWithStatus = objectMapper.writeValueAsString(reportPayloadDto);

            kafkaProducerService.sendMessage(KafkaTopics.EMAIL_NOTIFICATION.getTopicName(), reportPayloadWithStatus);
            kafkaProducerService.sendMessage(KafkaTopics.REPORT_STATUS.getTopicName(), CommonUtility.REPORT_DISPATCHED);

            acknowledgment.acknowledge();
        } catch (Exception e) {

            reportPayloadDto.setRetryCount(reportPayloadDto.getRetryCount() + 1);
            reportPayloadDto.setErrorCausedBy(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());

            try {
                reportPayloadWithStatus = objectMapper.writeValueAsString(reportPayloadDto);
            } catch (Exception ie) {
                ie.printStackTrace();
            }

            if (reportPayloadDto.getRetryCount() == 3) {
                kafkaProducerService.sendMessage(KafkaTopics.EMAIL_NOTIFICATION.getTopicName(), reportPayloadWithStatus);
                kafkaProducerService.sendMessage(KafkaTopics.REPORT_STATUS.getTopicName(), CommonUtility.REPORT_ERROR);
                acknowledgment.acknowledge();
            } else {
                kafkaProducerService.sendMessage(retryTopic, reportPayloadWithStatus);
            }
            e.printStackTrace();
        }
    }


    private static long getTimeTakenForReportProcess(long endTime, long startTime){
        return (endTime - startTime) / 1_000_000;
    }

    private static long getTotalMemoryUsedInKb(long usedMemoryAfter, long usedMemoryBefore){
        return (usedMemoryAfter - usedMemoryBefore) / 1024;
    }
}
