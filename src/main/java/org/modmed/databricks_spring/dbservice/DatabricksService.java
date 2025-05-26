package org.modmed.databricks_spring.dbservice;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class DatabricksService {

    private final String token = "Bearer dapiefdce23e0ba6af9afe0a4502ad639c4e";
    private final String host = "https://dbc-a7fd220b-9975.cloud.databricks.com";
    private final RestTemplate restTemplate = new RestTemplate();

    public List<Map<String, Object>> getNotebookResult() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        // STEP 1: Trigger the job
        String runNowUrl = host + "/api/2.1/jobs/run-now";
        String payload = """
        {
          "job_id": 1104986288668610
        }
        """;
        ResponseEntity<Map> response = restTemplate.postForEntity(runNowUrl, new HttpEntity<>(payload, headers), Map.class);
        Long parentRunId = ((Number) response.getBody().get("run_id")).longValue();
        System.out.println("parentRunId: " + parentRunId);

        // STEP 2: Poll top-level job status to get task run_id
        String runGetUrl = host + "/api/2.1/jobs/runs/get?run_id=" + parentRunId;
        Long taskRunId = null;

        for (int i = 0; i < 10; i++) {
            ResponseEntity<Map> runResponse = restTemplate.exchange(runGetUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            Map<String, Object> body = runResponse.getBody();
            Map<String, Object> state = (Map<String, Object>) body.get("state");
            String lifeCycle = (String) state.get("life_cycle_state");

            if ("TERMINATED".equalsIgnoreCase(lifeCycle)) {
                List<Map<String, Object>> tasks = (List<Map<String, Object>>) body.get("tasks");
                if (tasks == null || tasks.isEmpty()) {
                    throw new RuntimeException("No tasks found in run.");
                }
                taskRunId = ((Number) tasks.get(0).get("run_id")).longValue();
                System.out.println("TaskRunId: " + taskRunId);
                break;
            }

            Thread.sleep(13000);  // Wait 1 m: 3 seconds
        }

        if (taskRunId == null) {
            throw new RuntimeException("Failed to extract task run_id.");
        }
        //before this you have to create a notebook with a query
        // then create a job in job-run->jobs & pipeline
        //give task name
        // STEP 3: Get notebook_output from task run_id
        String taskRunUrl = host + "/api/2.1/jobs/runs/get-output?run_id=" + taskRunId;//changed here from get to get-output coz we want
        //the output of the current task, if any error go to databricks and check the job run
        System.out.println("taskRunUrl: " + taskRunUrl);
        ResponseEntity<Map> taskResp = restTemplate.exchange(taskRunUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);

        Map<String, Object> notebookOutput = (Map<String, Object>) taskResp.getBody().get("notebook_output");
        if (notebookOutput == null || notebookOutput.get("result") == null) {
            throw new RuntimeException("Notebook did not return any output.");
        }

        String result = (String) notebookOutput.get("result");
        System.out.println("result: " + result);

        // STEP 4: Parse and return JSON
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(response.getStatusCode());
        System.out.println(response.getBody());
        return mapper.readValue(result, new TypeReference<>() {});

    }
}
