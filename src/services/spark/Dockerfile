FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y wget curl iputils-ping postgresql-client

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P /opt/bitnami/spark/jars
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -P /opt/bitnami/spark/jars
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -P /opt/bitnami/spark/jars
RUN pip install python-dotenv pyspark numpy boto3 streamlit streamlit_mermaid rich pandas seaborn matplotlib scikit-learn scikit-optimize xgboost lightgbm joblib pycaret
