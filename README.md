# DataEngineerChallenge

Developed the application to process data and address the different business requirements.

## Processing & Analytical goals:

1. Sessionized the web log by IP. `com.paypay.solution.SessionizeByWebLog` has the solution

2. Determined the average session time. `com.paypay.solution.DataAnalysis` has the solution inside method getAverageSessionDurationPerUser

3. Determined unique URL visits per session. `com.paypay.solution.DataAnalysis` has the solution inside method getNumUniqueUrlsPerSession

4. Found the most engaged users. `com.paypay.solution.DataAnalysis` has the solution inside method getMostEngagedUsers

## Tools used:
- Spark 2.4.0 (with Scala 2.11.8)

## Testing:
- Test datasets have been placed the test class
- Test cases of all the data analysis have been placed in the code

## Running the application:
- Application was run locally using Intellij Idea IDE
- Objects `com.paypay.solution.SessionizeByWebLog` and `com.paypay.solution.DataAnalysis` have the main() methods which act as the entry point of running the application
- Application logs are displayed on stdout as well as saved inside `logs` directory relative to the project directory
- All the files were saved locally (as parquet files) inside `output` directory relative to the project directory and optionally printed on screen for debugging
- Saved files were analyzed using parquet-tools

## Deployment:
- We could build a jar and run the application
- Application can be updated to talk to the storage different from local (like: HDFS, S3, NoSQL store etc)

### Additional notes:
#### Libraries used: 
- logback for logging
- scalatest for writing test cases
- spark-core and spark-sql for running spark applications inside Intellij
- details are in the build.sbt file

#### Detecting distinct user
- Apart from ip addresses, userAgent has also been checked to establish uniqueness of a user.

## How this challenge was completed:

1. Forked the repo in github
2. Completed the processing and analytics as defined.
3. Placed notes in the code to help with clarity where appropriate.
4. Included the test code and data in your solution. 
5. Completed the work in own github repo and sent the results to be presented during interview.
