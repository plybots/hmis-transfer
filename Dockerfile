FROM python:3.9-slim
WORKDIR /app
COPY main.py .

RUN pip install requests

CMD [ "python", "main.py" ]
