FROM jupyter/pyspark-notebook:python-3.11
USER root

RUN apt-get update && \
    apt-get install -y curl vim wget git netcat graphviz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PIP_ROOT_USER_ACTION=ignore

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir tensorflow==2.15.0 && \
    pip install --no-cache-dir torch==2.0.1 && \
    pip install --no-cache-dir torchvision==0.15.2 && \
    pip install --no-cache-dir ipywidgets==8.1.1 flask==3.0.1 fastapi==0.115.5 uvicorn==0.32.0 pydot==2.0.0 && \
    pip install --no-cache-dir pennylane==0.39 && \ 
    pip install --no-cache-dir pandas==2.0.3 numpy==1.24.4 scipy==1.11.3 matplotlib==3.8.0 sympy==1.12 seaborn==0.13.0 && \
    pip install --no-cache-dir kafka-python==2.0.2 confluent_kafka==2.6.0 && \
    pip install --no-cache-dir pymongo==4.12.1 && \
    uvicorn[standard]==0.32.0 \
    pydantic<2.0 # Ważna zależność dla FastAPI \
    fix-permissions "/home/${NB_USER}" 

RUN wget https://archive.apache.org/dist/kafka/3.7.1/kafka_2.12-3.7.1.tgz && \
    tar -xzf kafka_2.12-3.7.1.tgz && \
    mv kafka_2.12-3.7.1 /home/jovyan/kafka

RUN rm -f /home/jovyan/kafka_2.12-3.7.1.tgz
RUN rm -rf ~/kafka/bin/windows/ ~/kafka/licenses ~/kafka/site-docs ~/kafka/NOTICE ~/kafka/LICENSE
RUN rm -f ~/kafka/bin/*server*.sh


USER ${NB_USER}

WORKDIR /home/jovyan/notebooks
