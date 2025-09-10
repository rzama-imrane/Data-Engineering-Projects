FROM python:3.9.10

RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet

WORKDIR /app

COPY upload_data.py upload_data.py



ENTRYPOINT [ "python" , "upload_data.py" ]