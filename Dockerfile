FROM public.ecr.aws/lambda/python:3.10

# Copy requirements file
COPY requirements-lambda.txt .

# Install the function's dependencies
RUN pip3 install -r requirements-lambda.txt --target "${LAMBDA_TASK_ROOT}"

# Install pyarrow for Parquet support
#RUN pip3 install pyarrow --target "${LAMBDA_TASK_ROOT}"

# Alternatively, you could install fastparquet instead
#RUN pip3 install fastparquet --target "${LAMBDA_TASK_ROOT}"
# I have moved these to requirements-lambda.txt

# Copy function code
COPY lambda/process-day/process-day.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD [ "process-day.lambda_handler" ]
