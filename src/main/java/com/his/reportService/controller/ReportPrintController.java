package com.his.reportService.controller;

import com.his.reportService.service.ReportPrintService;
import org.his.core.dto.ReportPayloadDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/generateReport")
public class ReportPrintController {
    @Autowired
    private ReportPrintService reportPrintService;


    @PostMapping("/print")
    public ResponseEntity<String> generateReport(@RequestBody ReportPayloadDto reportPayloadDto){
        try{
            reportPrintService.generateReportData(reportPayloadDto);
            return ResponseEntity.ok("Success");
        }catch (Exception e){
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }
}
