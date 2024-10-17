# snowflake-datalake-medallion-pipeline – breweries data 🍻

## **Project Overview**
This project implements an end-to-end data pipeline using Airflow to extract data from the [Open Brewery DB API](https://api.openbrewerydb.org/breweries) and store it in a Snowflake-based data lake following a medallion architecture (bronze, silver, and gold layers). The pipeline automates the extraction, transformation, and aggregation of brewery data for analytical purposes.

### Objective
The primary objective of this project is to demonstrate the ingestion and transformation of data using cloud-native and scalable services, ensuring data quality through validation checks, and generating insights via aggregated analysis.

![image](https://github.com/user-attachments/assets/879d3840-4e9f-4f74-b475-2986a16fd09b)

## Requirements
To successfully run and deploy the project, you will need:
- **Docker**🐳: To containerize the environment and ensure consistency.
- **Python 3.11**🐍: For the development and execution of Airflow tasks.
- **Snowflake**❄️: Used as the primary data lake.
- **AWS S3**🪣: For raw data storage and query management.
- **Airflow**🌬️: To orchestrate the pipeline.
- **Git**🌿: To manage the project repository.

## Deeper Overview of the Project
This project is structured using the **medallion architecture**, which consists of three main layers:
- **Bronze Layer** 🥉: Stores the raw data directly from the API in its original format (JSON).
- **Silver Layer** 🥈: Transforms and stores curated data in Snowflake, which natively manages data using columnar storage format.
- **Gold Layer** 🥇: Produces aggregated views of brewery data, showing the number of breweries by type and location.

## Solution Workflow
The core solutions work in tandem to ensure the data pipeline runs efficiently:
1. **API Data Extraction**: Data is fetched from the Open Brewery DB API and stored in the S3 bucket as raw JSON data.
2. **Data Lake Ingestion**: Raw data is loaded into the bronze layer in Snowflake using Snowflake operators.
3. **Data Transformation**: The silver layer applies transformations to clean and reformat the data, partitioning it by location.
4. **Aggregation**: Aggregated views are generated in the gold layer, summarizing the number of breweries by type and location for easier analysis.
5. **Data Quality Validation**: At each stage, the number of records inserted is validated against expectations, the api interactions are designed to deal with real values, and in case of errors, a structured cycle of retries is initiated to maintain a coherent flow throughout each step of the pipeline.
6. **Monitoring and Alerting**: If any task fails, alerts are sent via Slack for immediate resolution.

In addition to data ingestion and transformation, the project also focuses on **data quality validation** at each stage of the pipeline, with automated monitoring and alerting using Slack integration.

## **Setting up the Cloud Services & Alerts Setup Guide**

This guide provides detailed steps on setting up Amazon S3 for storage, Snowflake for data warehousing, and Slack for notifications from complete scratch.

## Amazon S3 Setup

### 1. Create an AWS Account
- Visit the [AWS Web Services](https://aws.amazon.com/pt/free/?gclid=CjwKCAjw68K4BhAuEiwAylp3krurUotDhtJ8MaFbYIvPFBU5fUZl0la7NqwVy4F7WJU3RAdOHYzOIhoC2MEQAvD_BwE&trk=c9dcfe7b-33fc-4345-b0c3-77b810bbd58c&sc_channel=ps&ef_id=CjwKCAjw68K4BhAuEiwAylp3krurUotDhtJ8MaFbYIvPFBU5fUZl0la7NqwVy4F7WJU3RAdOHYzOIhoC2MEQAvD_BwE:G:s&s_kwcid=AL!4422!3!454435137309!e!!g!!aws%20sign%20up!10758390156!106168762996&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc&awsf.Free%20Tier%20Types=*all&awsf.Free%20Tier%20Categories=*all) and sign up for a new account if you do not already have one.
- Follow the registration process, which includes providing your email address, billing information, and phone verification.

### 2. Generate an AWS Access Key and Secret Access Key
- Once your AWS account is created, navigate to the [IAM Management Console](https://console.aws.amazon.com/iam/).
- Select "Users" from the navigation pane, and then select your user name.
- In the user details pane, click on the "Security credentials" tab.
- Under the "Access keys" section, click "Create access key".
- Click "Download .csv file" to save the access key ID and secret access key to a secure location. This file contains your new access key ID and secret access key, which you will need for programmatic access to AWS services.

### 3. Access the S3 Management Console
- Log into the [AWS Management Console](https://aws.amazon.com/console/).
- Navigate to `Services` and select `S3` under the `Storage` category.

### 4. Create a New S3 Bucket
- In the S3 dashboard, click on `Create bucket`.
- Provide a name for your bucket. Remember that this will have to be the same bucket you'll interact with through Snowflake and Airflow. Same goes to the bucket paths.
- Select the region that is closest to your users to minimize latency.
- Configure the options of the bucket according to your needs.
- Create the following bucket paths inside your bucket: `airflow_stage/open_brewery_db/raw_api_responses/` to store the API responses, and `airflow_stage/open_brewery_db/queries/` to store your queries present in the project directory `project_assets/queries/pipeline_actions/airflow_s3_query_format`.

#### S3 Best practices:
- **Use Dedicated User Accounts**: Instead of operating as the root user, create dedicated user accounts with specific permissions. This approach helps minimize the risk of accidental disruptions or security breaches, as root users have unrestricted access that could lead to critical errors if misused.
- **Manage Permissions Carefully**: To control access to the bucket, configure permissions thoughtfully. For most applications, it is advisable to maintain the default settings, which block all public access, to safeguard your data.

#### Variables you'll need in your Airflow environment:
- `airflow_s3_bucket`: Your s3 bucket name.
- `open_brewery_db_api_response_path`: Path to the folders in which the jsons responses will be stored.
- `open_brewery_db_query_path`: Path where you place the queries the queries you use in the pipeline (copy the files from this folder `project_assets/queries/pipeline_actions/airflow_s3_query_format` to this path).

#### Connection you'll need in your Airflow environment:
- `s3_conn`: Create a connection of type "Amazon Web Services" fill the fields "*AWS Access Key ID*" and "AWS Secret Access Key" with your credentials.

## Snowflake Setup

### 1. Create a Snowflake Account
- Go to the [Snowflake website](https://www.snowflake.com/) and click on `Start for Free` which offers a trial account with credits.
- Fill in the necessary details to create your account.

### 2. Access the Snowflake Web Interface
- After signing up, access your Snowflake account through the web interface provided in the welcome email.
- Snowflake operates entirely in the cloud with no software installation required.

### 3. Familiarize Yourself with the Interface
- Explore the Snowflake interface to understand where different features and settings are located.
- Refer to the [Snowflake Documentation](https://docs.snowflake.com/en/) for comprehensive guides on navigating and using the platform.

## 4. Database Initialization:
- You can run the SQL scripts located in `/project_assets/queries/datalake_environment_set_up/` directly in Snowflake to set up your environment and initialize the bronze, silver, and gold layers.
- The file you should run first is `setting_up_snowflake_environment.sql`, it'll get you started with the `WAREHOUSE`, `DATABASE`, `SCHEMAS`, `TABLES` and `ROLES` necessary to run this project.
- Following that, run the commands in `setting_up_pipeline_elements` to enable the resources required for each stage of the pipeline in Snowflake. Both files include detailed comments and instructions—please read them thoroughly to ensure successful setup and operation.

#### Variables you'll need in your Airflow environment:
- `snowflake_database`: The database you created in Snowflake when running the environment setting up queries.

#### Connection you'll need in your Airflow environment:
- `snowflake_conn`: Create a connection of type "Snowflake" fill the fields "Login", "Password", "Warehouse", "Account", "Region, and "Role". You can leave the value "insecure_mode" in the extra as True, assuming you have the necessary permissions to operate securely within this configuration after setting up your AIRFLOW ROLE.

## Slack Setup

### 1. Create a Slack Account
- Visit [Slack’s website](https://slack.com/) and sign up for a new workspace or log into an existing one.

![image](https://github.com/user-attachments/assets/2cfcd673-4207-4604-b3e9-38d61864fab8)

### 2. Create a Slack App
- Navigate to the [Slack API page](https://api.slack.com/apps) and click on `Create New App`.
- Choose `From scratch`, name and customize your app, and select the workspace to add it to.

### 3. Configure Your Slack App
- In the app settings, navigate to `OAuth & Permissions`.
- Add necessary bot permissions such as `chat:write`, `chat:read`, and any other permissions your app might need.

### 4. Install the App to Your Workspace
- Scroll to the top of the `OAuth & Permissions` page and click on `Install App to Workspace`.
- Follow the prompts to authorize the app in your Slack workspace.

### 5. Obtain API Token
- After installing, you will be provided with a `Bot User OAuth Access Token`.
- Securely store this token as you will use it to interact with the Slack API programmatically.

#### Variable you'll need in your Airflow environment:
- `slack_secret_token`: This is basically the `Bot User OAuth Access Token` you took from the Slack API portal.

## **Setting Up the Project**
### 1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

### 2. Start Docker Containers:
Ensure Docker is running on your machine, then initialize the necessary services:
```bash
docker-compose up -d
```

### 3. Review the previews:
- Ensure that all the steps outlined in the `"Setting up the Cloud Services & Alerts Setup Guide"` have been successfully completed. Confirm that:
- The S3 bucket and its paths are correctly configured and align with the Airflow variables required for interaction.
- The Snowflake configurations, including role access, tables, and schemas, are properly set up and correspond to the necessary permissions, airflow variables, and structure needed for the pipeline to function effectively.

### 5. Execute Airflow:
- Access Airflow at `http://localhost:9090`.
- Try executing the DAG, if any error occurs review the steps.

## Running the Pipeline
Once everything is set up, the Airflow DAG is scheduled to run **daily at midnight**. However, you can manually trigger the DAG in the Airflow UI for testing or reprocessing.

### Main DAG Execution
The main DAG (`OPEN_BREWERY_DB_DATA_PIPELINE`) automates the entire workflow from data extraction to aggregation. Handling:
- **Raw Data Extraction**✨ (Bronze Layer) 
- **Data Transformation**🌟 (Silver Layer) 
- **Aggregated View Generation**⭐ (Gold Layer) 

If any task fails during execution, the monitoring system integrated with Slack will notify the team for immediate attention.

### When to Run
- **Daily Runs**: The pipeline is set to run daily, at midnight, but you can change the schedule cron expression according to your wish. Check [crontab.guru](https://crontab.guru/) to review custom cron expressions.
- **Manual Runs**: You can trigger a manual run via Airflow for testing or emergency reprocessing needs.

















