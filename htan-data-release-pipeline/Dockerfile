FROM python:3.11-slim

RUN apt-get update -y
RUN apt install curl -y 

ENV PATH=/google-cloud-sdk/bin:$PATH

RUN export CLOUD_SDK_VERSION="410.0.0" && \
    curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    ln -s /lib /lib64

WORKDIR /app

COPY htan-data-release-pipeline/ /app/

RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

CMD ["python3","run.py"]