postgres_config = {
    "host": "db",       # Địa chỉ Postgresql trong Docker Compose
    "port": 5432,              # Cổng mặc định của PostgreSQL là 5432
    "database": "ecommerce_db", # Tên cơ sở dữ liệu
    "user": "postgres",    # Tên người dùng database
    "password": "password", # Mật khẩu
    "sslmode": "prefer"        # Chế độ SSL (tùy chọn)
}