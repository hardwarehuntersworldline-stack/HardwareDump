package com.worldline.hardware.hunters.hardwaredump.controller;

import com.worldline.hardware.hunters.hardwaredump.service.BigQueryService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/bigquery")
public class BigQueryController {

    private final BigQueryService bigQueryService;

    public BigQueryController(BigQueryService bigQueryService) {
        this.bigQueryService = bigQueryService;
    }

    @GetMapping("/query")
    public String queryBigQuery(@RequestParam String dataset, @RequestParam String table) {
        return bigQueryService.runSimpleQuery(dataset, table);
    }
}
