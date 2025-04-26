-- 1. Create the email notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('ramasamypandiaraj.r@northeastern.edu');

-- 2. Create a tracking table to avoid duplicate alerts
CREATE OR REPLACE TABLE EVENTSLENS.EDW.EVENTLENS_PROCESSED_ALERTS (
    JOB_RUN_ID NUMBER NOT NULL,
    ALERT_SENT_TIME TIMESTAMP_NTZ,
    PRIMARY KEY (JOB_RUN_ID)
);

-- 3. Create a STREAM on the metrics table to track new inserts
CREATE OR REPLACE STREAM EVENTSLENS.EDW.EVENTLENS_METRICS_STREAM 
ON TABLE EVENTSLENS.EDW.EVENTLENS_METRICS;

-- 4. Stored Procedure to check for failed jobs and send alerts
CREATE OR REPLACE PROCEDURE EVENTSLENS.EDW.ALERT_ON_FAILED_JOBS()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var query = `
        SELECT 
            m.JOB_RUN_ID, 
            m.RUN_IDENTIFIER, 
            m.TASK_NAME, 
            m.START_TIME, 
            m.END_TIME, 
            m.STATUS,
            m.LOG_URL
        FROM EVENTSLENS.EDW.EVENTLENS_METRICS_STREAM m
        WHERE m.STATUS = 'FAILED'
          AND METADATA$ACTION = 'INSERT'
          AND NOT EXISTS (
              SELECT 1 FROM EVENTSLENS.EDW.EVENTLENS_PROCESSED_ALERTS p 
              WHERE p.JOB_RUN_ID = m.JOB_RUN_ID
          )
    `;

    var stmt = snowflake.createStatement({sqlText: query});
    var result = stmt.execute();

    var alertCount = 0;
    var processedIds = [];

    while (result.next()) {
        var jobRunId = result.getColumnValue(1);
        var runIdentifier = result.getColumnValue(2);
        var taskName = result.getColumnValue(3);
        var startTime = result.getColumnValue(4);
        var endTime = result.getColumnValue(5);
        var status = result.getColumnValue(6);
        var logUrl = result.getColumnValue(7);

        var subject = `ALERT: Failed Job - ${taskName}`;
        var body = `
A job has failed in the EventLens pipeline.

🔹 Task Name: ${taskName}
🔹 Run Identifier: ${runIdentifier}
🔹 Start Time: ${startTime}
🔹 End Time: ${endTime}
🔹 Status: ${status}
🔹 Job Run ID: ${jobRunId}
🔹 Log URL: ${logUrl || 'Not Available'}

Please investigate this issue.
        `;

        var sendEmailQuery = `
            CALL SYSTEM$SEND_EMAIL(
                'email_integration',
                'ramasamypandiaraj.r@northeastern.edu',
                '${subject.replace(/'/g, "''")}',
                '${body.replace(/'/g, "''")}'
            )
        `;
        snowflake.createStatement({sqlText: sendEmailQuery}).execute();

        var trackQuery = `
            INSERT INTO EVENTSLENS.EDW.EVENTLENS_PROCESSED_ALERTS (JOB_RUN_ID, ALERT_SENT_TIME)
            VALUES (${jobRunId}, CURRENT_TIMESTAMP())
        `;
        snowflake.createStatement({sqlText: trackQuery}).execute();

        alertCount++;
        processedIds.push(jobRunId);
    }

    return alertCount > 0 
        ? "✅ Alerts sent for failed jobs: " + processedIds.join(", ")
        : "✅ No failed jobs found in stream.";
$$;

-- 5. Create a task to run the procedure only when there’s new data in the stream
CREATE OR REPLACE TASK EVENTSLENS.EDW.MONITOR_FAILED_JOBS
  WAREHOUSE = EVENTSLENSMETRIC
  WHEN SYSTEM$STREAM_HAS_DATA('EVENTSLENS.EDW.EVENTLENS_METRICS_STREAM')
AS
  CALL EVENTSLENS.EDW.ALERT_ON_FAILED_JOBS();

-- 6. Activate the task
ALTER TASK EVENTSLENS.EDW.MONITOR_FAILED_JOBS RESUME;