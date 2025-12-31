## Notes and potential gotchas on using Code Ocean

- Any contents of the `data` directory in your capsule from your GitHub repository are not accessible in a pipeline
- Any folders outside of the `code` and `data` directories in your capsule are not accessible.
- When there are multiple mapped paths from one data asset to a capsule in default mode, each instance receives one file from each mapping
- The filter **/*.nwb does not work in Map Paths
- You cannot add the same data asset multiple times in a pipeline
- The ending `/` in a mapped path seems to make a difference. For example, mapping from `data/input/` vs `data/input` will lead to different behavior.
- When using external S3 data assets in a pipeline, the file is copied to the instance. In Map Paths "collect" mode, this means that all files are copied to the instance before the pipeline starts, which can be very slow for large datasets. This can lead to download failures.
- For a pipeline, you can configure Nextflow by creating a `nextflow.config` file in the `pipeline` directory. 
- Some configurations like `workflow.output.retryPolicy` do not work for some reason
- When publishing Zarr files, it is very easy to get a 503 Slow Down error from AWS. Reducing max connections, increasing max retries, and increasing the delay may help. Reducing the number of files, e.g., by increasing the chunk size for arrays, written may also help.
- When starting tasks in a pipeline, it is also very easy to get a 503 Slow Down error from AWS when cloning / accessing files in the capsule. Reducing the  `executor.submitRateLimit`, e.g. to '1/5s' may help but does not totally eliminate the issue.
- This pipeline configuration worked quite well but significantly increased the pipeline runtime: 
```
aws.client.maxConnections = 10 // Default is 50, reduce this value if needed
aws.client.maxRetries = 10 // Default is 5?
aws.client.delay = 2.s // Default is 350ms?
executor.submitRateLimit = '1/5s' // submit 1 job every 5 seconds to space out S3 requests
executor.queueSize = 50 // Default is 5000

aws {
    batch {
        // Retry S3 downloads up to 10 times if they fail, waiting 20 seconds to retry
        maxTransferAttempts = 10
        delayBetweenAttempts = 20
    }
}

process {
    // Calculate wait time: base delay * (multiplier ^ attempt_number)
    // Using Math.pow(2, task.attempt) for doubling delay
    // Wait 500ms, then 1000ms, 2000ms, etc
    errorStrategy = { 
        sleep(Math.pow(2, task.attempt) * 500 as long); return 'retry' 
    }
    maxRetries = 8
    maxErrors = 5
}
```
- Each task of a pipeline must generate a file. Otherwise, the task will be marked as failed.
- The Code Ocean App Builder does not support simple boolean command line arguments like `--metadata`. Instead, you must use a string argument with choices like `--metadata True/False`.
- After a pipeline run is complete, you can view the Nextflow Artifacts by clicking on the three dots next to the pipeline run in the Timeline tab. This can help with debugging.
