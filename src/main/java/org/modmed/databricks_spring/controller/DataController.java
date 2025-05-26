package org.modmed.databricks_spring.controller;

import org.modmed.databricks_spring.dbservice.DatabricksService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class DataController {

    @Autowired
    private DatabricksService databricksService;

    @GetMapping("/fetch-processed-data")
    public List<Map<String, Object>> fetchData() throws Exception {
        return databricksService.getNotebookResult();
    }
}
