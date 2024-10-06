import os
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_ecr_assets as ecr_assets,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
)
from constructs import Construct

class TimeSeriesExtractionStack(Stack):
    DATA_LOCATION = "MSG/MDSSFTD/NETCDF/"
    INTERMEDIATE_LOCATION = "output/intermediate/"
    OUTPUT_LOCATION = "output/final/"

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Reference the existing S3 bucket for input data
        bucket_data = s3.Bucket.from_bucket_name(
            self,
            "DataBucket",
            "satellite-weather-data-010928188967-us-east-1"
        )

        # Create a new bucket for storing Glue-related output
        bucket_glue = s3.Bucket(self, "GlueBucket")

        # Build the Docker image for process-day Lambda
        docker_asset_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        process_day_image = ecr_assets.DockerImageAsset(self, "ProcessDayImage",
            directory=docker_asset_path
        )

        # Create Lambda function from the Docker image
        process_day_lambda = _lambda.DockerImageFunction(
            self,
            "LambdaFunctionProcessDay",
            code=_lambda.DockerImageCode.from_ecr(process_day_image.repository, tag=process_day_image.image_tag),
            timeout=Duration.seconds(300),
            memory_size=2048,
            environment={
                "BUCKET_NAME": bucket_data.bucket_name,  # Use the existing bucket
                "INPUT_LOCATION": self.DATA_LOCATION,    # Data path: "MSG/MDSSFTD/NETCDF/"
                "OUTPUT_LOCATION": self.INTERMEDIATE_LOCATION,
            },
        )

        # Grant read/write permissions to the Lambda function on the S3 bucket
        bucket_data.grant_read_write(process_day_lambda)

        # Create generate-dates Lambda function (not using Docker)
        generate_dates_lambda = _lambda.Function(
            self,
            "LambdaFunctionGenerateDates",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="generate-dates.lambda_handler",
            code=_lambda.Code.from_asset("./lambda/generate-dates"),
        )

        # Step Functions to orchestrate processing
        map_json = {
            "Type": "Map",
            "ItemProcessor": {
                "ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
                "StartAt": "Process a Date",
                "States": {
                    "Process a Date": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::lambda:invoke",
                        "OutputPath": "$.Payload",
                        "Parameters": {
                            "Payload.$": "$",
                            "FunctionName": f"{process_day_lambda.function_arn}:$LATEST",
                        },
                        "Retry": [
                            {
                                "ErrorEquals": [
                                    "Lambda.ServiceException",
                                    "Lambda.AWSLambdaException",
                                    "Lambda.SdkClientException",
                                    "Lambda.TooManyRequestsException",
                                ],
                                "IntervalSeconds": 2,
                                "MaxAttempts": 6,
                                "BackoffRate": 2,
                            }
                        ],
                        "End": True,
                    }
                },
            },
            "End": True,
            "MaxConcurrency": 365,  # Concurrency for parallel execution of tasks
            "ToleratedFailurePercentage": 99,  # Increase the tolerated failure percentage if you expect some tasks to fail
            "ToleratedFailureCount": 357,  # Adjust the tolerated failure count if the number of expected failures is low
        }

        map_state = sfn.CustomState(self, "Map", state_json=map_json)

        dates_task = tasks.LambdaInvoke(
            self,
            "Generate List of Dates",
            lambda_function=generate_dates_lambda,
            payload_response_only=True,
        )

        step_function = sfn.StateMachine(
            self,
            "SFOrchestrateLambda",
            definition=sfn.Chain.start(dates_task).next(map_state),
        )

        # Grant necessary permissions to Step Functions
        step_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[
                    f"arn:aws:states:{Stack.of(self).region}:{Stack.of(self).account}:stateMachine:SFOrchestrateLambda*"
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        step_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    process_day_lambda.function_arn,
                    f"{process_day_lambda.function_arn}:*",
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        # Deploy Glue job source code to S3
        s3_deploy.BucketDeployment(
            self,
            "UploadCodeGlue",
            sources=[s3_deploy.Source.asset("./glue_src")],
            destination_bucket=bucket_glue,
            destination_key_prefix="src",
        )

        # Create IAM role for the Glue job
        glue_job_role = iam.Role(
            self, "GlueJobRole", assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )

        # Grant Glue job permissions to read from the input bucket and read/write to the Glue bucket
        bucket_glue.grant_read(glue_job_role)
        bucket_data.grant_read_write(glue_job_role)

        # Define the Glue job
        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            role=glue_job_role.role_name,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{bucket_glue.bucket_name}/src/glue_job.py",
            ),
            description="Glue job to repartition the data",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version="4.0",
            max_retries=0,
            number_of_workers=10,
            timeout=60,
            worker_type="G.1X",
            default_arguments={
                "--bucket_name": bucket_data.bucket_name,
                "--input_location": self.INTERMEDIATE_LOCATION,
                "--output_location": self.OUTPUT_LOCATION,
            },
        )
