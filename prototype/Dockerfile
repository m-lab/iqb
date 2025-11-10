# Streamlit Prototype Dockerfile for Cloud Run

FROM python:3.13-slim

# Install uv for faster dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy workspace configuration
COPY pyproject.toml uv.lock ./

# Copy library source (needed for workspace dependency)
COPY library/ ./library/

# Copy prototype source
COPY prototype/ ./prototype/

# Sync dependencies using uv (installs from lockfile)
RUN uv sync --frozen

# Expose port (Cloud Run will set PORT env var, defaulting to 8501)
ENV PORT=8501
EXPOSE ${PORT}

# Run Streamlit app
# --server.port uses PORT env var for Cloud Run compatibility
# --server.address=0.0.0.0 allows external connections
# --server.headless=true disables browser launch
WORKDIR /app/prototype
CMD ["/bin/sh", "-c", "uv run streamlit run Home.py --server.port=${PORT} --server.address=0.0.0.0 --server.headless=true"]
