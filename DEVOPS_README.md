# DevOps Infrastructure for E-commerce Data Platform

Comprehensive DevOps infrastructure for on-premises deployment of the E-commerce Data Platform, including CI/CD, monitoring, logging, container orchestration, and backup solutions.

## 📁 Directory Structure

```
.
├── .gitlab/                    # GitLab CI/CD setup
│   ├── docker-compose.gitlab.yaml
│   └── SETUP.md
├── monitoring/                 # Prometheus, Grafana, Alertmanager
│   ├── prometheus/
│   ├── grafana/
│   ├── alertmanager/
│   └── docker-compose.monitoring.yaml
├── logging/                    # Loki, Promtail
│   ├── loki/
│   ├── promtail/
│   └── docker-compose.logging.yaml
├── docker-swarm/              # Container orchestration
│   ├── setup-swarm.sh
│   ├── deploy-stack.sh
│   └── stack.yaml
├── vault/                     # Secret management
│   ├── config/
│   ├── policies/
│   └── docker-compose.vault.yaml
├── backup/                    # Backup & restore scripts
│   ├── scripts/
│   │   ├── backup-postgres.sh
│   ├──   ├── restore-postgres.sh
│   │   └── backup-minio.sh
│   └── docker-compose.backup.yaml
├── proxy/                     # Traefik reverse proxy
│   └── docker-compose.proxy.yaml
├── tests/                     # Testing infrastructure
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── .gitlab-ci.yml            # CI/CD pipeline definition
├── .pre-commit-config.yaml   # Pre-commit hooks
└── requirements-dev.txt      # Development dependencies
```

## 🚀 Quick Start

### 1. Setup GitLab CE (Optional - CI/CD)

```bash
# Start GitLab server
cd .gitlab
docker-compose -f docker-compose.gitlab.yaml up -d

# Follow setup guide
cat SETUP.md
```

### 2. Start Monitoring Stack

```bash
# Start Prometheus + Grafana
cd monitoring
docker-compose -f docker-compose.monitoring.yaml up -d

# Access Grafana: http://localhost:3000 (admin/admin123)
# Access Prometheus: http://localhost:9090
```

### 3. Start Logging Stack

```bash
# Start Loki + Promtail
cd logging
docker-compose -f docker-compose.logging.yaml up -d

# Query logs in Grafana: http://localhost:3000
```

### 4. Initialize Docker Swarm (Production)

```bash
# Initialize Swarm cluster
./docker-swarm/setup-swarm.sh

# Deploy stack
./docker-swarm/deploy-stack.sh
```

### 5. Setup Vault (Optional - Secrets)

```bash
cd vault
docker-compose -f docker-compose.vault.yaml up -d

# Access Vault UI: http://localhost:8200
```

## 📊 Access Points

| Service | Port | URL | Credentials |
|---------|------|-----|-------------|
| **Grafana** | 3000 | http://localhost:3000 | admin/admin123 |
| **Prometheus** | 9090 | http://localhost:9090 | None |
| **Alertmanager** | 9093 | http://localhost:9093 | None |
| **GitLab** | 80 | http://gitlab.local | root/[see setup] |
| **Vault** | 8200 | http://localhost:8200 | Token-based |
| **Traefik** | 8090 | http://localhost:8090 | admin/admin123 |

## 🔧 Key Features

### CI/CD Pipeline
- **5 Stages**: Lint, Test, Security, Build, Deploy
- **Linting**: Python (black, flake8), YAML, Docker
- **Testing**: Unit, Integration, E2E, DAG validation
- **Security**: Trivy container scanning
- **Deployment**: Automated to Dev/Staging/Production

### Monitoring & Observability
- **Metrics**: Prometheus with 15s scrape interval
- **Visualization**: Grafana with pre-configured dashboards
- **Alerts**: Alertmanager with email/Slack routing
- **Exporters**: Node, cAdvisor, PostgreSQL, Blackbox

### Centralized Logging
- **Aggregation**: Loki with 7-day retention
- **Collection**: Promtail for Docker containers and application logs
- **Integration**: Unified view in Grafana

### Backup & Recovery
- **PostgreSQL**: Daily full backups with 7-day retention
- **MinIO**: Incremental sync every 6 hours + weekly snapshots
- **Automation**: Scheduled via cron or Airflow DAGs

### Security
- **Secrets**: HashiCorp Vault integration
- **Scanning**: Trivy for container vulnerabilities
- **Access Control**: Traefik with basic auth
- **Pre-commit**: Automated security checks

## 📝 Common Operations

### Backup Database

```bash
# PostgreSQL backup
./backup/scripts/backup-postgres.sh

# MinIO backup
./backup/scripts/backup-minio.sh
```

### Restore Database

```bash
# PostgreSQL restore
./backup/scripts/restore-postgres.sh /backup/postgres/ecommerce_db_TIMESTAMP.sql.gz

# Optional: specify custom database name
./backup/scripts/restore-postgres.sh /path/to/backup.sql.gz custom_db
```

### View Logs

```bash
# Via Grafana Explore
# Navigate to http://localhost:3000 → Explore → Select Loki datasource

# Query examples:
# - All logs from Airflow: {job="airflow"}
# - Errors only: {job="airflow"} |= "ERROR"
# - Logs from specific container: {container="af_scheduler"}
```

### Check Alerts

```bash
# View active alerts
curl http://localhost:9093/api/v2/alerts

# Or check in Grafana: Alerting → Alert rules
```

### Deploy to Swarm

```bash
# Deploy entire stack
docker stack deploy -c docker-swarm/stack.yaml ecommerce

# Update specific service
docker service update ecommerce_airflow_api --image registry/airflow:latest

# Scale service
docker service scale ecommerce_spark_worker=3
```

## 🧪 Testing

### Run Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

### Run Tests Locally

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run DAG validation
python tests/test_dags_integrity.py
```

## 📚 Documentation

- **Implementation Plan**: See `implementation_plan.md` for detailed architecture and roadmap
- **GitLab Setup**: `.gitlab/SETUP.md`
- **Grafana Dashboards**: `monitoring/grafana/dashboards/README.md`

## 🔐 Security Best Practices

1. **Change Default Passwords**: Update all default credentials before production
2. **Enable SSL**: Configure Let's Encrypt in Traefik for HTTPS
3. **Vault Secrets**: Move all sensitive data to Vault
4. **Network Isolation**: Use separate networks for different stacks
5. **Regular Scanning**: Run Trivy scans on all container images
6. **Access Control**: Implement RBAC for GitLab, Grafana, Vault

## 🐛 Troubleshooting

### GitLab Runner not picking up jobs
```bash
docker exec -it gitlab-runner gitlab-runner verify
docker exec -it gitlab-runner gitlab-runner restart
```

### Prometheus not scraping targets
```bash
# Check targets status
curl http://localhost:9090/api/v1/targets | jq

# Verify network connectivity
docker exec -it prometheus ping <target-host>
```

### Loki not receiving logs
```bash
# Check Promtail status
docker logs promtail

# Verify Loki endpoint
curl http://localhost:3100/ready
```

## 📈 Performance Tuning

### Resource Limits
Adjust in docker-compose files:
```yaml
deploy:
  resources:
    limits:
      cpus: '2.0'
      memory: 2G
```

### Prometheus Retention
Default: 30 days. Adjust in `monitoring/docker-compose.monitoring.yaml`:
```yaml
command:
  - '--storage.tsdb.retention.time=60d'
```

### Loki Retention
Default: 7 days (168h). Adjust in `logging/loki/loki-config.yml`:
```yaml
limits_config:
  retention_period: 336h  # 14 days
```

## 🤝 Contributing

1. Create a feature branch
2. Make your changes
3. Run pre-commit hooks
4. Push and create a merge request in GitLab
5. Wait for CI pipeline to pass

## 📄 License

Apache 2.0 License

---

**Need Help?** Check the implementation plan or raise an issue in GitLab.
