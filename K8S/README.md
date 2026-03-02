# Kubernetes Deployment for E-commerce Data Platform

Thư mục này chứa các tài liệu cấu hình (Manifests) để triển khai toàn bộ nền tảng lên cụm Kubernetes (Sử dụng Minikube để phát triển cục bộ).

## 🏗 Cấu trúc thư mục

```text
K8S/
├── base/                  # Cấu trúc cơ bản (Namespace, Secrets, ConfigMaps)
├── databases/             # Dịch vụ Cơ sở dữ liệu (PostgreSQL)
└── gen-data/             # Dịch vụ giả lập dữ liệu (Gen Data)
```

## 🚀 Hướng dẫn triển khai (Local với Minikube)

### 1. Chuẩn bị môi trường
Đảm bảo bạn đã cài đặt và khởi động Minikube:
```bash
minikube start
```

> [!IMPORTANT]
> Do sử dụng `hostPath` để mount thư mục `init-db`, hãy đảm bảo đường dẫn trong `K8S/databases/postgres.yaml` chính xác với môi trường của bạn.

### 2. Triển khai base và database
```bash
# Tạo Namespace, Secrets và ConfigMaps
kubectl apply -f K8S/base/

# Triển khai PostgreSQL
kubectl apply -f K8S/databases/
```

### 3. Build Docker Image nội bộ
Để Kubernetes có thể sử dụng image bạn build trên máy mà không cần push lên Registry:
```bash
# Trỏ Docker CLI vào môi trường của Minikube
eval $(minikube docker-env)

# Build image cho gen_data
docker build -t ecommerce-gen-data:latest ./gen_data
```

### 3. Triển khai manifests
Áp dụng các cấu hình theo thứ tự:
```bash
# Tạo Namespace
kubectl apply -f K8S/base/

# Triển khai dịch vụ Gen Data
kubectl apply -f K8S/gen-data/
```

### 4. Kiểm tra trạng thái
```bash
kubectl get pods -n ecommerce
```

## 🖥 Quản lý trực quan với Portainer

Để quản lý cụm Minikube từ giao diện Portainer (đang chạy trên Docker), bạn cần cài đặt Portainer Agent vào trong cụm K8S:

1. **Triển khai Portainer Agent:**
```bash
kubectl apply -f https://downloads.portainer.io/ce2-19/portainer-agent-k8s.yaml
```

2. **Kết nối trên giao diện Portainer (Browser):**
- Truy cập Portainer của bạn (thường là `http://localhost:3000`).
- Vào mục **Environments** > **Add environment**.
- Chọn **Kubernetes** > **Agent**.
- **Name:** Minikube
- **Environment address:** `localhost:9001` (Đây là cổng mặc định của Portainer Agent).
- Nhấn **Connect**.

*Lưu ý: Nếu Portainer chạy trong Docker không thấy Minikube, bạn có thể cần dùng lệnh `kubectl port-forward` để đưa cổng 9001 của Agent ra ngoài máy host:*
```bash
kubectl port-forward svc/portainer-agent 9001:9001 -n portainer
```
