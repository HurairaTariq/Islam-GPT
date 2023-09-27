FROM python:3.10.0
RUN apt-get update -y
RUN apt-get autoremove -y
RUN apt-get clean

WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt .
RUN pip install --no-cache-dir --no-deps -r requirements.txt
RUN pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cpu
COPY . .
EXPOSE 5000
#CMD ["python3","-u","main.py"]