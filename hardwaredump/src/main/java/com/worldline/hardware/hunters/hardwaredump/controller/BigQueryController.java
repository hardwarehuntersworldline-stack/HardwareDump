package com.worldline.hardware.hunters.hardwaredump.controller;

import com.google.cloud.bigquery.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/bigquery")
public class BigQueryController {

    @GetMapping("/query")
    public ResponseEntity<?> queryBigQuery(
            @RequestParam String dataset,
            @RequestParam String table) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            String query = String.format("SELECT * FROM `%s.%s` LIMIT 10", dataset, table);
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult results = bigquery.query(queryConfig);

            List<Map<String, Object>> data = new ArrayList<>();
            for (FieldValueList row : results.iterateAll()) {
                Map<String, Object> rowData = new HashMap<>();
                for (Field field : results.getSchema().getFields()) {
                    rowData.put(field.getName(), row.get(field.getName()).getValue());
                }
                data.add(rowData);
            }

            return ResponseEntity.ok(data);
        } catch (BigQueryException bqe) {
            bqe.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "BigQuery error: " + bqe.getMessage()));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Unexpected error: " + e.getMessage()));
        }
    }
}
