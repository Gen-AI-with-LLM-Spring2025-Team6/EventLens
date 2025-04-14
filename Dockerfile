FROM python:3.12.7

# Install dependencies for wkhtmltopdf
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code

# Copy dependencies and install
COPY requirements.txt /code/
RUN pip install --no-cache-dir -r /code/requirements.txt

# Copy application code
COPY ./fastapi_backend /code/fastapi_backend
COPY ./streamlit_frontend /code/streamlit_frontend
COPY ./logging_module /code/logging_module

ENV PYTHONPATH="/code:${PYTHONPATH}"

# Expose necessary ports
EXPOSE 8000 8501

CMD ["/bin/bash", "-c", "uvicorn fastapi_backend.fast_api.fast_api:app --host 0.0.0.0 --port 8000 --reload & streamlit run /code/streamlit_frontend/streamlit_app.py --server.port 8501"]