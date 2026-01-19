FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

# Чтобы apt не задавал вопросов (tzdata и т.п.)
ENV DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Системные зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        git \
        ffmpeg \
        libgl1 \
        libglib2.0-0 \
        libsndfile1 \
        wget \
        unzip && \
    ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Клонируем SadTalker
RUN git clone https://github.com/OpenTalker/SadTalker.git

# --- Фикс конфликта scipy (как в рабочей версии) ---
RUN pip install --no-cache-dir scipy==1.10.1
RUN sed -i '/scipy==/d' SadTalker/requirements.txt SadTalker/requirements3d.txt
RUN pip install --no-cache-dir -r SadTalker/requirements.txt -r SadTalker/requirements3d.txt

# Скачиваем все модели SadTalker (чекпоинты, GFPGAN и пр.)
RUN cd SadTalker && \
    chmod +x scripts/download_models.sh && \
    bash scripts/download_models.sh

# Зависимости для веб-обёртки (FastAPI сервис)
RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    httpx \
    aiofiles \
    pydantic

# Код сервиса-обёртки
WORKDIR /app
COPY main.py /app/main.py

# Чтобы inference.py видел src/* из SadTalker
ENV PYTHONPATH=/workspace/SadTalker:${PYTHONPATH}

# /data монтируется volume'ом
RUN mkdir -p /dataц
VOLUME ["/data"]

EXPOSE 8102

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8102"]
