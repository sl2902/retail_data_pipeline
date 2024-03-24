FROM apache/airflow:2.8.1
COPY requirements.txt .
COPY .env .
RUN pip install apache-airflow==${AIRFLOW_VERSION}  -r requirements.txt

# RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# # Installing the package
# USER root
# RUN mkdir -p /usr/local/gcloud \
#   && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
#   && /usr/local/gcloud/google-cloud-sdk/install.sh

# # Adding the package path to local
# ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

# RUN mkdir -p /opt/airflow
# ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/
# this was failing with - failed to solve: rpc error: code = Unknown desc = failed to compute cache key: "/Users/home/Documents/secrets/personal-gcp.json" not found: not found
# COPY /Users/home/Documents/secrets/personal-gcp.json ${GOOGLE_APPLICATION_CREDENTIALS}

# RUN gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}


# USER airflow