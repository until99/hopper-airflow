# Use uma imagem Python slim como base
FROM python:3.10-slim-bookworm

# Copia o binário do 'uv' da imagem oficial (padrão mais eficiente)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Instala dependências do SO necessárias para Airflow + Postgres
# libpq-dev é essencial para o adaptador do Postgres
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Variáveis para o uv
# UV_COMPILE_BYTECODE=1 garante que o código seja compilado na instalação
# UV_LINK_MODE=copy garante que não usaremos hardlinks (melhor para Docker)
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# 1. Copia apenas os arquivos de dependência primeiro (para cache do Docker)
COPY pyproject.toml uv.lock ./

# 2. Instala as dependências
# --frozen: garante que instale exatamente o que está no lockfile
# --no-dev: não instala dependências de desenvolvimento
# --no-install-project: instala só as libs, não o seu código (ainda)
RUN uv sync --frozen --no-dev --no-install-project

# 3. Adiciona o virtualenv criado pelo uv ao PATH
# Isso faz com que 'python' e 'airflow' sejam chamados de dentro do venv automaticamente
ENV PATH="/app/.venv/bin:$PATH"

# 4. Copia o restante do código (seus DAGs, plugins, entrypoint)
COPY . .

# 5. Instala o próprio projeto (se configurado como pacote) ou finaliza o sync
RUN uv sync --frozen --no-dev

# Define a home do Airflow
ENV AIRFLOW_HOME=/app

# Permissão de execução no script de entrada
RUN chmod +x entrypoint.sh

EXPOSE 8080

CMD ["./entrypoint.sh"]