Bản triển khai mã nguồn đầy đủ cho toàn bộ dự án **Express Delivery Data Warehouse** theo chuẩn **Enterprise Medallion Architecture (`stg` -> `silver` -> `gold` -> `meta`) + Kimball Dimensional Modeling (3 Facts, 8 Dimensions)** trên nền tảng **PostgreSQL + Python + Apache Airflow + Docker**.

---

## 1. Cấu trúc thư mục chuẩn của dự án

```text
express-delivery-dwh/
├── config/
│   ├── db_config.py
│   └── settings.py
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql
│   │   └── 02_seed_constraints.sql
│   ├── dwh/
│   │   ├── 01_create_schemas.sql
│   │   ├── 02_create_meta_tables.sql
│   │   ├── 03_create_staging_tables.sql
│   │   ├── 04_create_silver_tables.sql
│   │   ├── 05_create_dimensions.sql
│   │   ├── 06_create_facts.sql
│   │   └── 07_create_indexes.sql
│   └── analytics/
│       ├── 01_create_views.sql
│       └── 02_create_materialized_views.sql
├── scripts/
│   ├── generate_oltp_data.py
│   ├── extract_incremental.py
│   ├── data_quality_checks.py
│   ├── load_dimensions.py
│   ├── load_facts.py
│   ├── refresh_views.py
│   └── run_local_pipeline.py
├── dags/
│   └── express_delivery_dwh_dag.py
└── docker/
    ├── docker-compose.yml
    ├── init-db.sql
    └── requirements.txt
```

---

## 2. Nhóm Cấu hình (`config/`)

### File: `config/settings.py`

```python
import os

# Cấu hình kết nối PostgreSQL OLTP và DWH (Hỗ trợ cả chạy Local và trong Docker)
OLTP_DB_URI = os.getenv(
    "OLTP_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
)

DWH_DB_URI = os.getenv(
    "DWH_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_dwh"
)

# Quy mô sinh dữ liệu mặc định
DEFAULT_CUSTOMERS = 1500
DEFAULT_HUBS = 30
DEFAULT_COURIERS = 200
DEFAULT_VEHICLES = 80
DEFAULT_SHIPMENTS = 10000

# Số vận đơn MỚI sinh thêm ở mỗi lần chạy mô phỏng incremental (lần chạy thứ 2 trở đi)
SIMULATE_NEW_SHIPMENTS = 200
```

### File: `config/db_config.py`

```python
from sqlalchemy import create_engine
from contextlib import contextmanager
from config.settings import OLTP_DB_URI, DWH_DB_URI

oltp_engine = create_engine(OLTP_DB_URI, pool_pre_ping=True, future=True)
dwh_engine = create_engine(DWH_DB_URI, pool_pre_ping=True, future=True)

@contextmanager
def get_oltp_conn():
    conn = oltp_engine.connect()
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_dwh_conn():
    conn = dwh_engine.connect()
    try:
        yield conn
    finally:
        conn.close()
```

---

## 3. Nhóm SQL Nguồn OLTP (`sql/oltp/`)

### File: `sql/oltp/01_create_oltp_tables.sql`

```sql
-- Tạo toàn bộ các bảng OLTP cốt lõi cho hệ thống Chuyển phát nhanh
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(30) NOT NULL UNIQUE,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,   -- Individual / Shop / B2B
    membership_tier VARCHAR(30) NOT NULL, -- Bronze / Silver / Gold
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hubs (
    hub_id SERIAL PRIMARY KEY,
    hub_code VARCHAR(30) NOT NULL UNIQUE,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,       -- Regional Hub / Local Branch
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    vehicle_code VARCHAR(30) NOT NULL UNIQUE,
    vehicle_type VARCHAR(50) NOT NULL,    -- Motorbike / Van / Truck_2T / Truck_8T
    capacity_kg NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS couriers (
    courier_id SERIAL PRIMARY KEY,
    courier_code VARCHAR(30) NOT NULL UNIQUE,
    courier_name VARCHAR(150) NOT NULL,
    hub_id INT REFERENCES hubs(hub_id),
    contract_type VARCHAR(30) NOT NULL,   -- Full-time / Part-time
    vehicle_id INT REFERENCES vehicles(vehicle_id),
    effective_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS service_types (
    service_type_id SERIAL PRIMARY KEY,
    service_code VARCHAR(30) NOT NULL UNIQUE,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    payment_method_code VARCHAR(30) NOT NULL UNIQUE,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,      -- Sender / Receiver / Third-Party
    has_cod BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS status_reasons (
    status_reason_id SERIAL PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) NOT NULL UNIQUE,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL,  -- Normal / Exception / Terminal
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    shipment_number VARCHAR(40) NOT NULL UNIQUE,
    sender_customer_id INT REFERENCES customers(customer_id),
    receiver_customer_id INT REFERENCES customers(customer_id),
    origin_hub_id INT REFERENCES hubs(hub_id),
    destination_hub_id INT REFERENCES hubs(hub_id),
    service_type_id INT REFERENCES service_types(service_type_id),
    payment_method_id INT REFERENCES payment_methods(payment_method_id),
    weight NUMERIC(10, 2) NOT NULL,       -- Cho phép gài số <= 0 ở tầng OLTP để test Quarantine
    distance_km NUMERIC(10, 2) DEFAULT 15.0,
    freight_charge NUMERIC(12, 2) NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    order_date TIMESTAMP NOT NULL,
    pickup_date TIMESTAMP,
    promised_delivery_date TIMESTAMP NOT NULL,
    actual_delivery_date TIMESTAMP,
    current_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipment_tracking_events (
    event_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES shipments(shipment_id),
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,      -- ORDER_CREATED, PICKED_UP, INBOUND_SCAN, OUTBOUND_SCAN, DELIVERY_ATTEMPT, DELIVERED, RETURNED
    hub_id INT REFERENCES hubs(hub_id),   -- Cho phép NULL để test DQ Warning -> Map Unknown (-1)
    courier_id INT REFERENCES couriers(courier_id), -- Cho phép NULL để test DQ Warning -> Map Unknown (-1)
    vehicle_id INT REFERENCES vehicles(vehicle_id),
    status_reason_id INT REFERENCES status_reasons(status_reason_id),
    scan_source VARCHAR(30) DEFAULT 'PDA_SCANNER',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_tickets (
    ticket_id SERIAL PRIMARY KEY,
    shipment_id BIGINT REFERENCES shipments(shipment_id),
    customer_id INT REFERENCES customers(customer_id),
    issue_type VARCHAR(50) NOT NULL,      -- DAMAGED_PARCEL, LATE_DELIVERY, WRONG_COD, ATTITUDE
    ticket_status VARCHAR(30) NOT NULL,   -- OPEN, IN_PROGRESS, RESOLVED
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cod_settlements (
    settlement_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES shipments(shipment_id),
    cod_amount NUMERIC(12, 2) NOT NULL,
    settlement_status VARCHAR(30) NOT NULL, -- PENDING / SETTLED
    settled_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### File: `sql/oltp/02_seed_constraints.sql`

```sql
-- Tạo Indexes trên cột updated_at của OLTP để tăng tốc Incremental Extract
CREATE INDEX IF NOT EXISTS idx_oltp_customers_updated ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_hubs_updated ON hubs(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_couriers_updated ON couriers(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_shipments_updated ON shipments(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_events_updated ON shipment_tracking_events(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_events_shipment ON shipment_tracking_events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_oltp_cod_updated ON cod_settlements(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_tickets_updated ON customer_tickets(updated_at);

-- [AUDIT-F3] Incremental Extract dựa hoàn toàn vào updated_at -> bảo đảm mọi UPDATE đều làm đổi updated_at.
-- Ứng dụng tự gán updated_at mới thì giữ nguyên; quên gán thì DB tự đặt thời điểm hiện tại.
CREATE OR REPLACE FUNCTION fn_touch_updated_at() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at := LOCALTIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers', 'hubs', 'vehicles', 'couriers', 'service_types', 'payment_methods',
                             'status_reasons', 'shipments', 'shipment_tracking_events', 'cod_settlements', 'customer_tickets']
    LOOP
        EXECUTE 'DROP TRIGGER IF EXISTS trg_touch_updated_at ON ' || quote_ident(t);
        EXECUTE 'CREATE TRIGGER trg_touch_updated_at BEFORE UPDATE ON ' || quote_ident(t)
             || ' FOR EACH ROW EXECUTE FUNCTION fn_touch_updated_at()';
    END LOOP;
END;
$$;

-- Seed dữ liệu danh mục tĩnh chuẩn (Service Types, Payment Methods, Status Reasons)
INSERT INTO service_types (service_code, service_name, sla_hours, is_express) VALUES
('STD_ECON', 'Chuyển phát Tiết kiệm', 72, FALSE),
('STD_FAST', 'Chuyển phát Nhanh Liên tỉnh', 48, FALSE),
('EXP_24H', 'Chuyển phát Hỏa tốc 24H', 24, TRUE),
('EXP_SAME', 'Giao Ngay Nội Thành (Same-Day)', 12, TRUE),
('HEAVY_CARGO', 'Vận tải Hàng nặng B2B', 96, FALSE)
ON CONFLICT (service_code) DO NOTHING;

INSERT INTO payment_methods (payment_method_code, payment_method_name, payer_type, has_cod) VALUES
('CASH_SENDER', 'Tiền mặt Người gửi trả', 'Sender', FALSE),
('COD_RECEIVER', 'Thu hộ COD & Phí người nhận trả', 'Receiver', TRUE),
('COD_FREE_SHIP', 'Thu hộ COD - Shop trả cước', 'Sender', TRUE),
('WALLET_PREPAID', 'Ví điện tử trả trước', 'Sender', FALSE),
('B2B_MONTHLY', 'Công nợ cuối tháng B2B', 'Sender', FALSE)
ON CONFLICT (payment_method_code) DO NOTHING;

INSERT INTO status_reasons (status_code, status_name, reason_code, reason_name, event_category) VALUES
('CREATED', 'Mới tạo vận đơn', 'RS_INIT', 'Khách hàng tạo đơn trên hệ thống', 'Normal'),
('PICKED_UP', 'Đã lấy hàng', 'RS_PICK_OK', 'Bưu tá lấy hàng thành công tại điểm gửi', 'Normal'),
('IN_TRANSIT', 'Đang trung chuyển', 'RS_HUB_IN', 'Hàng nhập kho trung tâm phân loại (Inbound)', 'Normal'),
('IN_TRANSIT', 'Đang xuất kho', 'RS_HUB_OUT', 'Hàng xuất kho đi tuyến tiếp theo (Outbound)', 'Normal'),
('DELIVERING', 'Đang giao hàng', 'RS_OUT_DELIV', 'Shipper đang đi giao chặng cuối', 'Normal'),
('DELIVERED', 'Giao thành công', 'RS_DELIV_OK', 'Người nhận đã ký nhận hàng', 'Terminal'),
('DELIVERY_FAILED', 'Giao thất bại', 'RS_CUST_BUSY', 'Khách hẹn giao lại ca sau / Không nghe máy', 'Exception'),
('RETURNED', 'Hoàn hàng', 'RS_REFUSED', 'Người nhận từ chối nhận hàng', 'Terminal'),
('RETURNED', 'Hoàn hàng', 'RS_WRONG_ADDR', 'Sai địa chỉ / Số điện thoại không liên lạc được', 'Terminal'),
('CANCELLED', 'Đã hủy', 'RS_SHOP_CANCEL', 'Người gửi yêu cầu hủy đơn trước khi lấy', 'Terminal')
ON CONFLICT (reason_code) DO NOTHING;
```

---

## 4. Nhóm SQL Kho dữ liệu 3 Tầng (`sql/dwh/`)

### File: `sql/dwh/01_create_schemas.sql`

```sql
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;
```

### File: `sql/dwh/02_create_meta_tables.sql`

```sql
CREATE TABLE IF NOT EXISTS meta.etl_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_extracted_timestamp TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00',
    last_batch_id VARCHAR(64),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO meta.etl_watermark (table_name, last_extracted_timestamp) VALUES
('customers', '1970-01-01 00:00:00'),
('hubs', '1970-01-01 00:00:00'),
('vehicles', '1970-01-01 00:00:00'),
('couriers', '1970-01-01 00:00:00'),
('service_types', '1970-01-01 00:00:00'),
('payment_methods', '1970-01-01 00:00:00'),
('status_reasons', '1970-01-01 00:00:00'),
('shipments', '1970-01-01 00:00:00'),
('shipment_tracking_events', '1970-01-01 00:00:00'),
('cod_settlements', '1970-01-01 00:00:00'),
('customer_tickets', '1970-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS meta.etl_batch_log (
    batch_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    run_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_end TIMESTAMP,
    rows_extracted INT DEFAULT 0,
    rows_loaded INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    status VARCHAR(30) NOT NULL, -- RUNNING / EXTRACTED / SUCCESS / FAILED
    error_message TEXT,
    source_snapshot_ts TIMESTAMP, -- [AUDIT-A3] Thời điểm snapshot OLTP của batch -> watermark mới khi batch SUCCESS
    PRIMARY KEY (batch_id, table_name)
);
-- Bổ sung cột cho DB đã tạo từ phiên bản cũ
ALTER TABLE meta.etl_batch_log ADD COLUMN IF NOT EXISTS source_snapshot_ts TIMESTAMP;

CREATE TABLE IF NOT EXISTS meta.data_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    rule_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- Blocker / Warning
    error_message TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE
);
```

### File: `sql/dwh/03_create_staging_tables.sql`

```sql
-- Tầng Bronze / Staging: Lưu dữ liệu thô theo batch_id để kiểm toán và cô lập lỗi
CREATE TABLE IF NOT EXISTS stg.customers (
    customer_id INT,
    customer_code VARCHAR(30),
    customer_name VARCHAR(150),
    customer_type VARCHAR(30),
    membership_tier VARCHAR(30),
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.hubs (
    hub_id INT,
    hub_code VARCHAR(30),
    hub_name VARCHAR(150),
    hub_level VARCHAR(50),
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.vehicles (
    vehicle_id INT,
    vehicle_code VARCHAR(30),
    vehicle_type VARCHAR(50),
    capacity_kg NUMERIC(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.couriers (
    courier_id INT,
    courier_code VARCHAR(30),
    courier_name VARCHAR(150),
    hub_id INT,
    contract_type VARCHAR(30),
    vehicle_id INT,
    effective_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.service_types (
    service_type_id INT,
    service_code VARCHAR(30),
    service_name VARCHAR(100),
    sla_hours INT,
    is_express BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.payment_methods (
    payment_method_id INT,
    payment_method_code VARCHAR(30),
    payment_method_name VARCHAR(100),
    payer_type VARCHAR(50),
    has_cod BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.status_reasons (
    status_reason_id INT,
    status_code VARCHAR(30),
    status_name VARCHAR(100),
    reason_code VARCHAR(30),
    reason_name VARCHAR(150),
    event_category VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.shipments (
    shipment_id BIGINT,
    shipment_number VARCHAR(40),
    sender_customer_id INT,
    receiver_customer_id INT,
    origin_hub_id INT,
    destination_hub_id INT,
    service_type_id INT,
    payment_method_id INT,
    weight NUMERIC(10, 2),
    distance_km NUMERIC(10, 2),
    freight_charge NUMERIC(12, 2),
    cod_amount NUMERIC(12, 2),
    order_date TIMESTAMP,
    pickup_date TIMESTAMP,
    promised_delivery_date TIMESTAMP,
    actual_delivery_date TIMESTAMP,
    current_status VARCHAR(30),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.shipment_tracking_events (
    event_id BIGINT,
    shipment_id BIGINT,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    hub_id INT,
    courier_id INT,
    vehicle_id INT,
    status_reason_id INT,
    scan_source VARCHAR(30),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.cod_settlements (
    settlement_id BIGINT,
    shipment_id BIGINT,
    cod_amount NUMERIC(12, 2),
    settlement_status VARCHAR(30),
    settled_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.customer_tickets (
    ticket_id INT,
    shipment_id BIGINT,
    customer_id INT,
    issue_type VARCHAR(50),
    ticket_status VARCHAR(30),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### File: `sql/dwh/04_create_silver_tables.sql`

```sql
-- Tầng Silver / Cleansed & Conformed: Đã qua Data Quality Gate, chuẩn hóa chuỗi và tính sẵn row_hash
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id INT PRIMARY KEY,
    customer_code VARCHAR(30) NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    membership_tier VARCHAR(30) NOT NULL,
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    row_hash VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.hubs (
    hub_id INT PRIMARY KEY,
    hub_code VARCHAR(30) NOT NULL,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.vehicles (
    vehicle_id INT PRIMARY KEY,
    vehicle_code VARCHAR(30) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.couriers (
    courier_id INT PRIMARY KEY,
    courier_code VARCHAR(30) NOT NULL,
    courier_name VARCHAR(150) NOT NULL,
    hub_id INT,
    contract_type VARCHAR(30) NOT NULL,
    vehicle_id INT,
    effective_date DATE,   -- [AUDIT-C2] Ngày hiệu lực nghiệp vụ (điều chuyển Hub...) -> mốc valid_from của SCD2
    row_hash VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.service_types (
    service_type_id INT PRIMARY KEY,
    service_code VARCHAR(30) NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.payment_methods (
    payment_method_id INT PRIMARY KEY,
    payment_method_code VARCHAR(30) NOT NULL,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,
    has_cod BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.status_reasons (
    status_reason_id INT PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) NOT NULL,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.shipments (
    shipment_id BIGINT PRIMARY KEY,
    shipment_number VARCHAR(40) NOT NULL UNIQUE,
    sender_customer_id INT NOT NULL,
    -- [AUDIT-B3] Các FK phụ cho phép NULL (OLTP cho phép NULL) -> DQ Warning + map Unknown (SK = -1) ở Gold,
    -- thay vì NOT NULL làm sập toàn bộ batch khi gặp 1 dòng thiếu
    receiver_customer_id INT,
    origin_hub_id INT,
    destination_hub_id INT,
    service_type_id INT,
    payment_method_id INT,
    weight NUMERIC(10, 2) NOT NULL,
    distance_km NUMERIC(10, 2) NOT NULL,
    freight_charge NUMERIC(12, 2) NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    pickup_date TIMESTAMP,
    promised_delivery_date TIMESTAMP NOT NULL,
    actual_delivery_date TIMESTAMP,
    current_status VARCHAR(30) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.shipment_tracking_events (
    event_id BIGINT PRIMARY KEY,
    shipment_id BIGINT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    hub_id INT,        -- Cho phép NULL để map sang SK = -1 ở tầng Gold
    courier_id INT,    -- Cho phép NULL để map sang SK = -1 ở tầng Gold
    vehicle_id INT,
    status_reason_id INT,
    scan_source VARCHAR(30),
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.cod_settlements (
    settlement_id BIGINT PRIMARY KEY,
    shipment_id BIGINT NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL,
    settlement_status VARCHAR(30) NOT NULL,
    settled_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.customer_tickets (
    ticket_id INT PRIMARY KEY,
    shipment_id BIGINT,   -- [AUDIT-B7] OLTP cho phép ticket không gắn vận đơn; Gold chỉ dùng ticket có shipment_id
    customer_id INT,
    issue_type VARCHAR(50) NOT NULL,
    ticket_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);
```

### File: `sql/dwh/05_create_dimensions.sql`

```sql
-- Tầng Gold: 8 Dimensions theo thiết kế Kimball Star Schema (Không Snowflaking) kèm dòng Unknown (SK = -1)

-- 1. Dim_Date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_peak_season BOOLEAN NOT NULL
);

INSERT INTO gold.dim_date VALUES
(-1, '1900-01-01', 1900, 1, 1, 'Unknown', 1, 1, 1, FALSE, FALSE)
ON CONFLICT (date_key) DO NOTHING;

-- [AUDIT-D4] Hàm tra date_key an toàn cho mọi Fact: NULL hoặc ngày nằm ngoài lịch Dim_Date -> -1 (Unknown)
-- (trước đây TO_CHAR(...)::INT trực tiếp -> 1 ngày ngoài khoảng 2024-2028 là vỡ FK và hỏng cả batch)
CREATE OR REPLACE FUNCTION gold.fn_date_key(p_ts TIMESTAMP)
RETURNS INT
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        (SELECT d.date_key FROM gold.dim_date d
         WHERE d.date_key = TO_CHAR(p_ts, 'YYYYMMDD')::INT),
        -1
    );
$$;

-- 2. Dim_Hub (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_hub (
    hub_sk SERIAL PRIMARY KEY,
    hub_nk VARCHAR(30) NOT NULL UNIQUE,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO gold.dim_hub (hub_sk, hub_nk, hub_name, hub_level, city, district, capacity, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Hub', 'Unknown', 'Unknown', 'Unknown', 0, TRUE)
ON CONFLICT (hub_nk) DO NOTHING;

-- 3. Dim_Vehicle (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
    vehicle_sk SERIAL PRIMARY KEY,
    vehicle_nk VARCHAR(30) NOT NULL UNIQUE,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg NUMERIC(10, 2) NOT NULL
);

INSERT INTO gold.dim_vehicle (vehicle_sk, vehicle_nk, vehicle_type, capacity_kg)
VALUES (-1, 'UNKNOWN', 'Unknown Vehicle', 0)
ON CONFLICT (vehicle_nk) DO NOTHING;

-- 4. Dim_Service_Type (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_service_type (
    service_type_sk SERIAL PRIMARY KEY,
    service_type_nk VARCHAR(30) NOT NULL UNIQUE,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL
);

INSERT INTO gold.dim_service_type (service_type_sk, service_type_nk, service_name, sla_hours, is_express)
VALUES (-1, 'UNKNOWN', 'Unknown Service', 0, FALSE)
ON CONFLICT (service_type_nk) DO NOTHING;

-- 5. Dim_Status_Reason (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_status_reason (
    status_reason_sk SERIAL PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) NOT NULL UNIQUE,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL
);

INSERT INTO gold.dim_status_reason (status_reason_sk, status_code, status_name, reason_code, reason_name, event_category)
VALUES (-1, 'UNKNOWN', 'Unknown Status', 'UNKNOWN', 'Unknown Reason', 'Unknown')
ON CONFLICT (reason_code) DO NOTHING;

-- 6. Dim_Payment_Method (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_payment_method (
    payment_method_sk SERIAL PRIMARY KEY,
    payment_method_nk VARCHAR(30) NOT NULL UNIQUE,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,
    has_cod BOOLEAN NOT NULL
);

INSERT INTO gold.dim_payment_method (payment_method_sk, payment_method_nk, payment_method_name, payer_type, has_cod)
VALUES (-1, 'UNKNOWN', 'Unknown Payment Method', 'Unknown', FALSE)
ON CONFLICT (payment_method_nk) DO NOTHING;

-- 7. Dim_Customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_nk VARCHAR(30) NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    membership_tier VARCHAR(30) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash VARCHAR(64) NOT NULL
);

INSERT INTO gold.dim_customer (customer_sk, customer_nk, customer_name, customer_type, membership_tier, city, district, valid_from, valid_to, is_current, row_hash)
SELECT -1, 'UNKNOWN', 'Unknown Customer', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01 00:00:00', '9999-12-31 23:59:59', TRUE, 'UNKNOWN'
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer WHERE customer_sk = -1);

-- 8. Dim_Courier (SCD Type 2 - Đã Denormalize thông tin Hub và Vehicle để chống lỗi Snowflaking)
CREATE TABLE IF NOT EXISTS gold.dim_courier (
    courier_sk SERIAL PRIMARY KEY,
    courier_nk VARCHAR(30) NOT NULL,
    courier_name VARCHAR(150) NOT NULL,
    hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    assigned_hub_code VARCHAR(30) NOT NULL DEFAULT 'UNKNOWN',
    assigned_hub_name VARCHAR(150) NOT NULL DEFAULT 'Unknown Hub',
    assigned_hub_city VARCHAR(100) NOT NULL DEFAULT 'Unknown',
    contract_type VARCHAR(30) NOT NULL,
    vehicle_sk INT NOT NULL REFERENCES gold.dim_vehicle(vehicle_sk),
    assigned_vehicle_type VARCHAR(50) NOT NULL DEFAULT 'Unknown Vehicle',
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash VARCHAR(64) NOT NULL
);

INSERT INTO gold.dim_courier (
    courier_sk, courier_nk, courier_name,
    hub_sk, assigned_hub_code, assigned_hub_name, assigned_hub_city,
    contract_type, vehicle_sk, assigned_vehicle_type,
    valid_from, valid_to, is_current, row_hash
)
SELECT
    -1, 'UNKNOWN', 'Unknown Courier',
    -1, 'UNKNOWN', 'Unknown Hub', 'Unknown',
    'Unknown', -1, 'Unknown Vehicle',
    '1900-01-01 00:00:00', '9999-12-31 23:59:59', TRUE, 'UNKNOWN'
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_courier WHERE courier_sk = -1);
```

### File: `sql/dwh/06_create_facts.sql`

```sql
-- Tầng Gold: 3 Bảng Fact chuẩn Kimball Galaxy Schema

-- Fact 1: Accumulating Snapshot Fact - Theo dõi toàn trình vận đơn, SLA, COD và Khiếu nại
CREATE TABLE IF NOT EXISTS gold.fact_shipment_lifecycle (
    shipment_sk BIGSERIAL PRIMARY KEY,
    shipment_id_nk BIGINT NOT NULL UNIQUE,       -- [AUDIT-D9] Khóa nguồn bất biến, dùng làm khóa upsert
    shipment_number VARCHAR(40) NOT NULL UNIQUE, -- Degenerate Dimension (có thể đổi ở nguồn -> được cập nhật theo)
    sender_customer_sk INT NOT NULL REFERENCES gold.dim_customer(customer_sk),
    receiver_customer_sk INT NOT NULL REFERENCES gold.dim_customer(customer_sk),
    origin_hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    destination_hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    service_type_sk INT NOT NULL REFERENCES gold.dim_service_type(service_type_sk),
    payment_method_sk INT NOT NULL REFERENCES gold.dim_payment_method(payment_method_sk),
    current_status_sk INT NOT NULL REFERENCES gold.dim_status_reason(status_reason_sk),

    -- Role-playing Date Keys
    order_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    pickup_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    first_sorting_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    last_sorting_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    first_delivery_attempt_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    final_delivery_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    return_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    promised_delivery_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    cod_settled_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),

    -- Milestone Timestamps chính xác tới từng giây để phân tích khung giờ cao điểm
    order_timestamp TIMESTAMP NOT NULL,
    pickup_timestamp TIMESTAMP,
    first_sorting_timestamp TIMESTAMP,
    last_sorting_timestamp TIMESTAMP,
    first_delivery_attempt_timestamp TIMESTAMP,
    final_delivery_timestamp TIMESTAMP,
    promised_delivery_timestamp TIMESTAMP NOT NULL,
    cod_settled_timestamp TIMESTAMP,

    -- Additive Measures
    weight NUMERIC(10, 2) NOT NULL,
    freight_charge NUMERIC(12, 2) NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL,
    distance_km NUMERIC(10, 2) NOT NULL,
    delivery_attempt_count INT NOT NULL DEFAULT 0,
    ticket_count INT NOT NULL DEFAULT 0,

    -- Lag / Lead-time Measures (Hours)
    pickup_to_sorting_hours NUMERIC(10, 2),
    sorting_to_delivery_hours NUMERIC(10, 2),
    total_lead_time_hours NUMERIC(10, 2),

    -- Operational & SLA Flags
    is_delivered BOOLEAN NOT NULL DEFAULT FALSE,
    is_returned BOOLEAN NOT NULL DEFAULT FALSE,
    is_cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    is_on_time BOOLEAN NOT NULL DEFAULT FALSE,
    is_sla_breached BOOLEAN NOT NULL DEFAULT FALSE,
    is_cod_settled BOOLEAN NOT NULL DEFAULT FALSE,
    has_complaint_ticket BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(64) NOT NULL
);

-- Fact 2: Transaction Fact - Sự kiện quét mã vạch trên hành trình
-- Đã bổ sung Degenerate Dimension shipment_number và bỏ ràng buộc FK cứng vào Fact_Shipment_Lifecycle
CREATE TABLE IF NOT EXISTS gold.fact_tracking_events (
    tracking_event_sk BIGSERIAL PRIMARY KEY,
    event_id_nk BIGINT NOT NULL UNIQUE,
    shipment_sk BIGINT NOT NULL DEFAULT -1,
    shipment_number VARCHAR(40) NOT NULL, -- Degenerate Dimension chuẩn Galaxy Schema
    event_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    courier_sk INT NOT NULL REFERENCES gold.dim_courier(courier_sk),
    vehicle_sk INT NOT NULL REFERENCES gold.dim_vehicle(vehicle_sk),
    status_reason_sk INT NOT NULL REFERENCES gold.dim_status_reason(status_reason_sk),

    event_sequence INT NOT NULL DEFAULT 1,
    scan_source VARCHAR(30),
    batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Fact 3: Periodic Snapshot Fact - Chốt tồn đọng và lưu lượng Hub theo ngày
CREATE TABLE IF NOT EXISTS gold.fact_daily_hub_snapshot (
    snapshot_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),

    inbound_count INT NOT NULL DEFAULT 0,
    outbound_count INT NOT NULL DEFAULT 0,
    backlog_count INT NOT NULL DEFAULT 0,
    pending_delivery_count INT NOT NULL DEFAULT 0,
    return_count INT NOT NULL DEFAULT 0,
    cod_held_amount NUMERIC(14, 2) NOT NULL DEFAULT 0,

    batch_id VARCHAR(64) NOT NULL,
    snapshot_generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_date_key, hub_sk)
);
```

### File: `sql/dwh/07_create_indexes.sql`

```sql
-- Indexes cho Dimensions (SCD2 & Natural Keys)
CREATE INDEX IF NOT EXISTS idx_dim_cust_nk_valid ON gold.dim_customer(customer_nk, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dim_cust_current ON gold.dim_customer(customer_nk, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_cour_nk_valid ON gold.dim_courier(courier_nk, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dim_cour_current ON gold.dim_courier(courier_nk, is_current);

-- Indexes cho Fact_Tracking_Events
CREATE INDEX IF NOT EXISTS idx_fte_event_date ON gold.fact_tracking_events(event_date_key);
CREATE INDEX IF NOT EXISTS idx_fte_hub_date ON gold.fact_tracking_events(hub_sk, event_date_key);
CREATE INDEX IF NOT EXISTS idx_fte_courier_date ON gold.fact_tracking_events(courier_sk, event_date_key);
CREATE INDEX IF NOT EXISTS idx_fte_shipment_sk ON gold.fact_tracking_events(shipment_sk);
CREATE INDEX IF NOT EXISTS idx_fte_shipment_num ON gold.fact_tracking_events(shipment_number);

-- Indexes cho Fact_Shipment_Lifecycle
CREATE INDEX IF NOT EXISTS idx_fsl_order_date ON gold.fact_shipment_lifecycle(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fsl_deliv_date ON gold.fact_shipment_lifecycle(final_delivery_date_key);
CREATE INDEX IF NOT EXISTS idx_fsl_hubs ON gold.fact_shipment_lifecycle(origin_hub_sk, destination_hub_sk);
CREATE INDEX IF NOT EXISTS idx_fsl_service ON gold.fact_shipment_lifecycle(service_type_sk);
CREATE INDEX IF NOT EXISTS idx_fsl_status ON gold.fact_shipment_lifecycle(current_status_sk);

-- Indexes cho Fact_Daily_Hub_Snapshot
CREATE INDEX IF NOT EXISTS idx_fdhs_date ON gold.fact_daily_hub_snapshot(snapshot_date_key);
CREATE INDEX IF NOT EXISTS idx_fdhs_hub_date ON gold.fact_daily_hub_snapshot(hub_sk, snapshot_date_key);

-- [AUDIT-E1] Indexes cho stg / silver / meta: mọi bước ETL đều lọc theo batch_id và tra cứu theo shipment_id
CREATE INDEX IF NOT EXISTS idx_stg_shipments_batch ON stg.shipments(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_batch ON stg.shipment_tracking_events(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_cod_batch ON stg.cod_settlements(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_tickets_batch ON stg.customer_tickets(batch_id);
CREATE INDEX IF NOT EXISTS idx_sil_shipments_batch ON silver.shipments(batch_id);
CREATE INDEX IF NOT EXISTS idx_sil_events_batch ON silver.shipment_tracking_events(batch_id);
CREATE INDEX IF NOT EXISTS idx_sil_events_shipment ON silver.shipment_tracking_events(shipment_id, event_time);
CREATE INDEX IF NOT EXISTS idx_sil_cod_shipment ON silver.cod_settlements(shipment_id);
CREATE INDEX IF NOT EXISTS idx_sil_tickets_shipment ON silver.customer_tickets(shipment_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_lookup ON meta.data_quarantine(batch_id, source_table, severity, record_id);
```

---

## 5. Nhóm SQL Semantic Layer & Analytics (`sql/analytics/`)

### File: `sql/analytics/01_create_views.sql`

```sql
-- View 1: Tổng hợp SLA, OTD, COD & Khiếu nại phục vụ Trang 1 Dashboard (Executive & SLA Overview)
-- DROP trước: CREATE OR REPLACE VIEW không cho chèn cột mới vào giữa danh sách cột của view đã tồn tại
DROP VIEW IF EXISTS gold.v_slaship_summary;
CREATE VIEW gold.v_slaship_summary AS
SELECT
    d.full_date AS order_date,
    d.year,
    d.month,
    d.month_name,
    st.service_name,
    st.is_express,
    ho.hub_name AS origin_hub,
    ho.city AS origin_city,
    hd.hub_name AS destination_hub,
    hd.city AS destination_city,
    cs.customer_type AS sender_type,
    cs.membership_tier AS sender_tier,
    sr.status_code AS current_status_code,
    sr.reason_name AS latest_status_reason,
    COUNT(f.shipment_sk) AS total_shipments,
    SUM(f.freight_charge) AS total_freight_revenue,
    SUM(f.cod_amount) AS total_cod_amount,
    SUM(CASE WHEN f.is_cod_settled THEN f.cod_amount ELSE 0 END) AS settled_cod_amount,
    SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END) AS delivered_shipments,
    SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END) AS returned_shipments,
    SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END) AS cancelled_shipments,
    SUM(CASE WHEN f.is_delivered AND f.is_on_time THEN 1 ELSE 0 END) AS on_time_shipments,
    SUM(CASE WHEN f.is_sla_breached THEN 1 ELSE 0 END) AS sla_breached_shipments,
    -- [AUDIT-V1] Đơn CHƯA kết thúc nhưng đã quá hạn hẹn giao, tính tại thời điểm truy vấn
    -- (không lưu trong accumulating snapshot vì cờ này đổi theo thời gian dù đơn không phát sinh thay đổi nào)
    SUM(CASE WHEN NOT (f.is_delivered OR f.is_returned OR f.is_cancelled)
              AND f.promised_delivery_timestamp < LOCALTIMESTAMP THEN 1 ELSE 0 END) AS overdue_open_shipments,
    SUM(f.ticket_count) AS total_complaint_tickets,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_delivered AND f.is_on_time THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END), 0),
        2
    ) AS otd_percentage,
    ROUND(AVG(f.total_lead_time_hours), 2) AS avg_total_lead_time_hours
FROM gold.fact_shipment_lifecycle f
JOIN gold.dim_date d ON f.order_date_key = d.date_key
JOIN gold.dim_service_type st ON f.service_type_sk = st.service_type_sk
JOIN gold.dim_hub ho ON f.origin_hub_sk = ho.hub_sk
JOIN gold.dim_hub hd ON f.destination_hub_sk = hd.hub_sk
JOIN gold.dim_customer cs ON f.sender_customer_sk = cs.customer_sk
JOIN gold.dim_status_reason sr ON f.current_status_sk = sr.status_reason_sk
GROUP BY
    d.full_date, d.year, d.month, d.month_name,
    st.service_name, st.is_express,
    ho.hub_name, ho.city,
    hd.hub_name, hd.city,
    cs.customer_type, cs.membership_tier,
    sr.status_code, sr.reason_name;
```

### File: `sql/analytics/02_create_materialized_views.sql`

```sql
-- ============================================================================
-- File: sql/analytics/02_create_materialized_views.sql
-- Mô tả: Tầng Semantic / Materialized Views phục vụ BI Dashboard (Trang 2 & 3)
-- Đã vá lỗi:
--   1. Chống nhân bản dòng (Fan-trap) khi đơn hàng ra/vào cùng 1 Hub nhiều lần
--      bằng cách dùng Window Function LEAD() ghép cặp INBOUND -> OUTBOUND liền kề.
--   2. Sử dụng các thuộc tính Denormalized trên Dim_Courier (SCD2) để giữ đúng lịch sử.
-- ============================================================================

-- Materialized View 1: Hiệu năng & Tắc nghẽn Hub (Dwell Time + Backlog) cho Trang 2 Dashboard
DROP MATERIALIZED VIEW IF EXISTS gold.mv_daily_hub_performance;

CREATE MATERIALIZED VIEW gold.mv_daily_hub_performance AS
WITH ordered_hub_events AS (
    -- Sắp xếp tuần tự các sự kiện tại từng Hub của mỗi vận đơn để ghép cặp chính xác
    SELECT
        e.shipment_number,
        e.hub_sk,
        e.event_date_key,
        e.event_type,
        e.event_timestamp,
        LEAD(e.event_type) OVER (
            PARTITION BY e.shipment_number, e.hub_sk
            ORDER BY e.event_timestamp, e.event_sequence
        ) AS next_event_type,
        LEAD(e.event_timestamp) OVER (
            PARTITION BY e.shipment_number, e.hub_sk
            ORDER BY e.event_timestamp, e.event_sequence
        ) AS next_event_timestamp
    FROM gold.fact_tracking_events e
    WHERE e.event_type IN ('INBOUND_SCAN', 'OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
      AND e.hub_sk <> -1
),
hub_dwell AS (
    -- Chỉ tính thời gian lưu kho (Dwell Time) giữa cặp INBOUND_SCAN và sự kiện rời kho liền kề
    SELECT
        hub_sk,
        event_date_key AS snapshot_date_key,
        AVG(
            EXTRACT(EPOCH FROM (next_event_timestamp - event_timestamp)) / 3600.0
        ) AS avg_dwell_time_hours
    FROM ordered_hub_events
    WHERE event_type = 'INBOUND_SCAN'
      AND next_event_type IN ('OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
      AND next_event_timestamp >= event_timestamp
    GROUP BY hub_sk, event_date_key
)
SELECT
    d.full_date AS snapshot_date,
    d.year,
    d.month,
    h.hub_nk AS hub_code,
    h.hub_name,
    h.hub_level,
    h.city,
    h.district,
    h.capacity,
    s.inbound_count,
    s.outbound_count,
    s.backlog_count,
    s.pending_delivery_count,
    s.return_count,
    s.cod_held_amount,
    ROUND(COALESCE(dw.avg_dwell_time_hours, 0)::NUMERIC, 2) AS avg_dwell_time_hours,
    ROUND(
        (100.0 * s.backlog_count / NULLIF(h.capacity, 0))::NUMERIC,
        2
    ) AS backlog_capacity_ratio_pct
FROM gold.fact_daily_hub_snapshot s
JOIN gold.dim_date d ON s.snapshot_date_key = d.date_key
JOIN gold.dim_hub h ON s.hub_sk = h.hub_sk
LEFT JOIN hub_dwell dw
  ON s.hub_sk = dw.hub_sk
 AND s.snapshot_date_key = dw.snapshot_date_key
WHERE h.hub_sk <> -1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hub_perf_unique
ON gold.mv_daily_hub_performance(snapshot_date, hub_code);


-- ============================================================================
-- Materialized View 2: Hiệu suất Shipper & Phân tích Hoàn hàng (Trang 3 Dashboard)
-- Chứng minh giá trị lịch sử của SCD Type 2 trên Dim_Courier (theo từng phiên bản courier_sk)
-- ============================================================================
DROP MATERIALIZED VIEW IF EXISTS gold.mv_courier_performance;

CREATE MATERIALIZED VIEW gold.mv_courier_performance AS
SELECT
    d.year,
    d.month,
    c.courier_sk,
    c.courier_nk AS courier_code,
    c.courier_name,
    c.contract_type,
    c.valid_from AS version_valid_from,
    c.valid_to AS version_valid_to,
    c.is_current AS is_current_version,
    c.assigned_hub_code AS assigned_hub_code_at_event,
    c.assigned_hub_name AS assigned_hub_at_event,
    c.assigned_hub_city AS assigned_hub_city,
    c.assigned_vehicle_type AS assigned_vehicle_type,
    COUNT(DISTINCT CASE
        WHEN e.event_type IN ('DELIVERED', 'RETURNED', 'DELIVERY_ATTEMPT') THEN e.shipment_number
    END) AS total_handled_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'PICKED_UP' THEN e.shipment_number
    END) AS total_picked_up_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'DELIVERED' THEN e.shipment_number
    END) AS delivered_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'RETURNED' THEN e.shipment_number
    END) AS returned_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_number
    END) AS on_time_delivered_shipments,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_number END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_number END), 0),
        2
    ) AS courier_otd_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_number END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED') THEN e.shipment_number END), 0),
        2
    ) AS courier_return_rate_pct
FROM gold.fact_tracking_events e
JOIN gold.fact_shipment_lifecycle f ON e.shipment_number = f.shipment_number
JOIN gold.dim_date d ON e.event_date_key = d.date_key
JOIN gold.dim_courier c ON e.courier_sk = c.courier_sk
WHERE c.courier_sk <> -1
GROUP BY
    d.year,
    d.month,
    c.courier_sk,
    c.courier_nk,
    c.courier_name,
    c.contract_type,
    c.valid_from,
    c.valid_to,
    c.is_current,
    c.assigned_hub_code,
    c.assigned_hub_name,
    c.assigned_hub_city,
    c.assigned_vehicle_type;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_courier_perf_unique
ON gold.mv_courier_performance(year, month, courier_sk);
```

---

## 6. Nhóm Scripts ETL & Sinh dữ liệu (`scripts/`)

### File: `scripts/generate_oltp_data.py`

```python
import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import text
from config.db_config import get_oltp_conn
from config.settings import (
    DEFAULT_CUSTOMERS, DEFAULT_HUBS, DEFAULT_COURIERS,
    DEFAULT_VEHICLES, DEFAULT_SHIPMENTS, SIMULATE_NEW_SHIPMENTS
)

fake = Faker('vi_VN')
Faker.seed(42)   # [AUDIT-F4] Seed cả Faker để dữ liệu tái lập được giữa các lần dựng lại
random.seed(42)

CITIES = [
    ("Hà Nội", ["Cầu Giấy", "Đống Đa", "Hoàng Mai", "Long Biên", "Nam Từ Liêm"]),
    ("TP. Hồ Chí Minh", ["Quận 1", "Quận 7", "Tân Bình", "Thủ Đức", "Bình Thạnh"]),
    ("Đà Nẵng", ["Hải Châu", "Thanh Khê", "Sơn Trà", "Ngũ Hành Sơn"]),
    ("Hải Phòng", ["Ngô Quyền", "Lê Chân", "Hồng Bàng"]),
    ("Cần Thơ", ["Ninh Kiều", "Bình Thủy", "Cái Răng"])
]

# [AUDIT-F1] Không sinh mốc thời gian nào ở tương lai: mọi timestamp <= thời điểm sinh dữ liệu - SAFETY_MARGIN
SAFETY_MARGIN = timedelta(minutes=5)


# =====================================================================
# HÀM DÙNG CHUNG CHO LẦN SINH ĐẦU TIÊN VÀ CÁC LẦN MÔ PHỎNG INCREMENTAL
# =====================================================================
def _load_lookups(conn):
    hub_ids = [r[0] for r in conn.execute(text("SELECT hub_id FROM hubs ORDER BY hub_id")).fetchall()]
    couriers_by_hub = {}
    courier_ids = []
    for courier_id, hub_id in conn.execute(text("SELECT courier_id, hub_id FROM couriers ORDER BY courier_id")).fetchall():
        courier_ids.append(courier_id)
        couriers_by_hub.setdefault(hub_id, []).append(courier_id)
    return {
        "hub_ids": hub_ids,
        "vehicle_ids": [r[0] for r in conn.execute(text("SELECT vehicle_id FROM vehicles ORDER BY vehicle_id")).fetchall()],
        "cust_ids": [r[0] for r in conn.execute(text("SELECT customer_id FROM customers ORDER BY customer_id")).fetchall()],
        "courier_ids": courier_ids,
        "couriers_by_hub": couriers_by_hub,
        "services": conn.execute(text("SELECT service_type_id, sla_hours FROM service_types ORDER BY service_type_id")).fetchall(),
        "payments": conn.execute(text("SELECT payment_method_id, has_cod FROM payment_methods ORDER BY payment_method_id")).fetchall(),
        "reasons": {r[0]: r[1] for r in conn.execute(text("SELECT reason_code, status_reason_id FROM status_reasons")).fetchall()},
    }


def _pick_courier(lk, hub_id):
    # [AUDIT-F5] Bưu tá được chọn trong số bưu tá thuộc đúng Hub phát sinh sự kiện (trước đây chọn ngẫu nhiên toàn mạng)
    return random.choice(lk["couriers_by_hub"].get(hub_id) or lk["courier_ids"])


def _build_shipments(lk, first_number, count, order_from, order_to, gen_now, audit_ts=None):
    """Sinh vận đơn. Trả về (rows để INSERT, timeline sạch theo shipment_number để sinh sự kiện).
    audit_ts: nếu có, created_at/updated_at = thời điểm GHI bản ghi (như OLTP thật), không phải thời gian nghiệp vụ.
    [AUDIT-F7] Bắt buộc với dữ liệu chèn ở các lần chạy incremental: CDC dựa trên updated_at sẽ không bao giờ thấy
    bản ghi mới có updated_at lùi về quá khứ (đo được: chỉ 25/200 đơn mới được extract)."""
    limit = gen_now - SAFETY_MARGIN
    rows, timeline = [], {}
    span_sec = max((order_to - order_from).total_seconds(), 1)

    for k in range(count):
        shipment_number = f"EXP{first_number + k}"
        sender_id, receiver_id = random.sample(lk["cust_ids"], 2)
        orig_hub, dest_hub = random.sample(lk["hub_ids"], 2)
        srv_id, sla_hours = random.choice(lk["services"])
        pay_id, has_cod = random.choice(lk["payments"])

        order_dt = order_from + timedelta(seconds=random.uniform(0, span_sec))
        promised_dt = order_dt + timedelta(hours=sla_hours)
        # [AUDIT-F2] Lấy hàng luôn trước hạn giao (tối đa 25% SLA) -> timeline vận đơn luôn hợp lệ
        pickup_dt = order_dt + timedelta(hours=random.uniform(0.5, min(6.0, sla_hours * 0.25)))
        actual_deliv_dt = None

        rand_status = random.random()
        if rand_status < 0.78:
            current_status = "DELIVERED"
            if random.random() < 0.85:
                # Đúng hạn: giao trong khoảng [pickup, promised]
                actual_deliv_dt = pickup_dt + (promised_dt - pickup_dt) * random.uniform(0.35, 0.95)
            else:
                actual_deliv_dt = promised_dt + timedelta(hours=random.uniform(2, 36))
        elif rand_status < 0.86:
            current_status = "RETURNED"
            actual_deliv_dt = pickup_dt + timedelta(hours=random.uniform(24, 72))
        elif rand_status < 0.96:
            current_status = "IN_TRANSIT"
        else:
            current_status = "CANCELLED"
            pickup_dt = None

        # [AUDIT-F1] Mốc kết thúc rơi vào tương lai -> ngoài đời thực đơn này vẫn đang trên đường
        if actual_deliv_dt is not None and actual_deliv_dt > limit:
            current_status, actual_deliv_dt = "IN_TRANSIT", None
        # Đơn vừa đặt, chưa tới giờ lấy hàng
        if pickup_dt is not None and pickup_dt > limit:
            current_status, pickup_dt = "CREATED", None

        if current_status in ("DELIVERED", "RETURNED"):
            updated_dt = actual_deliv_dt
        elif current_status == "IN_TRANSIT":
            updated_dt = pickup_dt + timedelta(hours=random.uniform(2, 10))
        elif current_status == "CANCELLED":
            updated_dt = order_dt + timedelta(minutes=30)
        else:
            updated_dt = order_dt
        updated_dt = min(updated_dt, limit)

        timeline[shipment_number] = {
            "status": current_status, "order": order_dt, "pickup": pickup_dt, "end": actual_deliv_dt
        }

        weight = round(random.uniform(0.2, 25.0), 2)
        # CÀI CẮM DỮ LIỆU BẨN (1% weight <= 0, 1% delivery_date < order_date) - chỉ làm bẩn bản ghi vận đơn,
        # timeline sự kiện vẫn sinh từ mốc sạch ở trên
        dirty_roll = random.random()
        if dirty_roll < 0.01:
            weight = round(random.uniform(-10.0, 0.0), 2)
        elif dirty_roll < 0.02 and actual_deliv_dt is not None:
            actual_deliv_dt = order_dt - timedelta(hours=5)

        rows.append({
            "shipment_number": shipment_number,
            "sender_customer_id": sender_id,
            "receiver_customer_id": receiver_id,
            "origin_hub_id": orig_hub,
            "destination_hub_id": dest_hub,
            "service_type_id": srv_id,
            "payment_method_id": pay_id,
            "weight": weight,
            "distance_km": round(random.uniform(5.0, 1200.0), 1),
            "freight_charge": round(20000 + weight * 4500, 2) if weight > 0 else 25000,
            "cod_amount": round(random.randint(10, 200) * 10000, 2) if has_cod else 0.0,
            "order_date": order_dt,
            "pickup_date": pickup_dt,
            "promised_delivery_date": promised_dt,
            "actual_delivery_date": actual_deliv_dt,
            "current_status": current_status,
            "created_at": audit_ts or order_dt,
            "updated_at": audit_ts or updated_dt
        })
    return rows, timeline


def _insert_shipments(conn, rows):
    conn.execute(text("""
        INSERT INTO shipments (
            shipment_number, sender_customer_id, receiver_customer_id,
            origin_hub_id, destination_hub_id, service_type_id, payment_method_id,
            weight, distance_km, freight_charge, cod_amount,
            order_date, pickup_date, promised_delivery_date, actual_delivery_date,
            current_status, created_at, updated_at
        ) VALUES (
            :shipment_number, :sender_customer_id, :receiver_customer_id,
            :origin_hub_id, :destination_hub_id, :service_type_id, :payment_method_id,
            :weight, :distance_km, :freight_charge, :cod_amount,
            :order_date, :pickup_date, :promised_delivery_date, :actual_delivery_date,
            :current_status, :created_at, :updated_at
        )
    """), rows)
    return conn.execute(text("""
        SELECT shipment_id, shipment_number, sender_customer_id, origin_hub_id, destination_hub_id, cod_amount
        FROM shipments WHERE shipment_number = ANY(:nums)
    """), {"nums": [r["shipment_number"] for r in rows]}).fetchall()


def _build_children(lk, inserted, timeline, gen_now, audit_ts=None):
    """Sinh sự kiện hành trình, COD và ticket theo timeline sạch của từng vận đơn."""
    limit = gen_now - SAFETY_MARGIN
    reasons = lk["reasons"]
    event_rows, cod_rows, ticket_rows = [], [], []

    def add_event(sh_id, ts, event_type, hub_id, courier_id, vehicle_id, reason_code, source):
        if ts is None or ts > limit:   # [AUDIT-F1] không sinh sự kiện ở tương lai
            return
        event_rows.append({
            "shipment_id": sh_id, "event_time": ts, "event_type": event_type,
            "hub_id": hub_id, "courier_id": courier_id, "vehicle_id": vehicle_id,
            "status_reason_id": reasons[reason_code], "scan_source": source,
            "created_at": audit_ts or ts, "updated_at": audit_ts or ts
        })

    for sh_id, sh_number, sender_id, orig_h, dest_h, cod_amt in inserted:
        tl = timeline[sh_number]
        st_code, ord_dt, pick_dt, end_dt = tl["status"], tl["order"], tl["pickup"], tl["end"]
        c1, c2 = _pick_courier(lk, orig_h), _pick_courier(lk, dest_h)
        v1, v2 = random.sample(lk["vehicle_ids"], 2)

        # Event 1: ORDER_CREATED
        add_event(sh_id, ord_dt, "ORDER_CREATED", orig_h, c1, v1, "RS_INIT", "CUSTOMER_APP")

        if st_code == "CANCELLED":
            add_event(sh_id, ord_dt + timedelta(minutes=30), "CANCELLED", orig_h, c1, v1, "RS_SHOP_CANCEL", "CUSTOMER_APP")
            continue
        if st_code == "CREATED":
            continue

        # Event 2: PICKED_UP (Cài cắm 2% thiếu courier_id, 1% thiếu hub_id)
        ev_hub = None if random.random() < 0.01 else orig_h
        ev_courier = None if random.random() < 0.02 else c1
        add_event(sh_id, pick_dt, "PICKED_UP", ev_hub, ev_courier, v1, "RS_PICK_OK", "PDA_SCANNER")

        # [AUDIT-F2] Các mốc trung chuyển được đặt theo tỉ lệ trong khoảng [pickup, kết thúc]
        #   -> MỌI đơn DELIVERED/RETURNED đều có đủ INBOUND/OUTBOUND/INBOUND đích/sự kiện kết thúc theo đúng thứ tự
        #   (trước đây ~16% đơn DELIVERED không có sự kiện DELIVERED vì giao "trước" khi xuất kho)
        if end_dt is not None:
            span = end_dt - pick_dt
            in_orig_dt = pick_dt + span * random.uniform(0.05, 0.15)
            out_orig_dt = in_orig_dt + span * random.uniform(0.10, 0.25)
            in_dest_dt = out_orig_dt + span * random.uniform(0.20, 0.35)
        else:
            in_orig_dt = pick_dt + timedelta(hours=random.uniform(1.0, 3.0))
            out_orig_dt = in_orig_dt + timedelta(hours=random.uniform(1.5, 6.0))
            in_dest_dt = None

        add_event(sh_id, in_orig_dt, "INBOUND_SCAN", orig_h, c1, v1, "RS_HUB_IN", "HUB_GATE_SCAN")
        add_event(sh_id, out_orig_dt, "OUTBOUND_SCAN", orig_h, c1, v2, "RS_HUB_OUT", "HUB_GATE_SCAN")

        if end_dt is not None:
            add_event(sh_id, in_dest_dt, "INBOUND_SCAN", dest_h, c2, v2, "RS_HUB_IN", "HUB_GATE_SCAN")
            # Giao thất bại trước khi thành công/hoàn: 12% đơn giao thành công, mọi đơn hoàn có 1-2 lần
            n_fail = (1 if random.random() < 0.12 else 0) if st_code == "DELIVERED" else random.choice([1, 2])
            for k in range(n_fail):
                attempt_dt = in_dest_dt + (end_dt - in_dest_dt) * (k + 1) / (n_fail + 1)
                add_event(sh_id, attempt_dt, "DELIVERY_ATTEMPT", dest_h, c2, v2, "RS_CUST_BUSY", "PDA_SCANNER")
            final_reason = "RS_DELIV_OK" if st_code == "DELIVERED" else random.choice(["RS_REFUSED", "RS_WRONG_ADDR"])
            add_event(sh_id, end_dt, st_code, dest_h, c2, v2, final_reason, "PDA_SCANNER")

        if cod_amt > 0:
            settled_dt = None
            if st_code == "DELIVERED" and random.random() < 0.7:
                settled_dt = end_dt + timedelta(days=1)
                if settled_dt > limit:   # [AUDIT-F1] chưa tới ngày đối soát
                    settled_dt = None
            cod_rows.append({
                "shipment_id": sh_id,
                "cod_amount": cod_amt,
                "settlement_status": "SETTLED" if settled_dt else "PENDING",
                "settled_at": settled_dt,
                "created_at": audit_ts or ord_dt,
                "updated_at": audit_ts or settled_dt or ord_dt
            })

        # Sinh khoảng 5% đơn có phát sinh Ticket khiếu nại
        if random.random() < 0.05:
            ticket_dt = min(ord_dt + timedelta(hours=12), limit)
            ticket_rows.append({
                "shipment_id": sh_id,
                "customer_id": sender_id,
                "issue_type": random.choice(["LATE_DELIVERY", "DAMAGED_PARCEL", "WRONG_COD", "ATTITUDE"]),
                "ticket_status": random.choice(["OPEN", "IN_PROGRESS", "RESOLVED"]),
                "created_at": audit_ts or ticket_dt,
                "updated_at": audit_ts or ticket_dt
            })

    return event_rows, cod_rows, ticket_rows


def _insert_children(conn, event_rows, cod_rows, ticket_rows):
    batch_size = 5000
    for idx in range(0, len(event_rows), batch_size):
        conn.execute(text("""
            INSERT INTO shipment_tracking_events (
                shipment_id, event_time, event_type, hub_id, courier_id,
                vehicle_id, status_reason_id, scan_source, created_at, updated_at
            ) VALUES (
                :shipment_id, :event_time, :event_type, :hub_id, :courier_id,
                :vehicle_id, :status_reason_id, :scan_source, :created_at, :updated_at
            )
        """), event_rows[idx:idx + batch_size])

    if cod_rows:
        conn.execute(text("""
            INSERT INTO cod_settlements (shipment_id, cod_amount, settlement_status, settled_at, created_at, updated_at)
            VALUES (:shipment_id, :cod_amount, :settlement_status, :settled_at, :created_at, :updated_at)
        """), cod_rows)

    if ticket_rows:
        conn.execute(text("""
            INSERT INTO customer_tickets (shipment_id, customer_id, issue_type, ticket_status, created_at, updated_at)
            VALUES (:shipment_id, :customer_id, :issue_type, :ticket_status, :created_at, :updated_at)
        """), ticket_rows)


def _next_shipment_number(conn):
    return conn.execute(text("""
        SELECT COALESCE(MAX(SUBSTRING(shipment_number FROM 4)::BIGINT), 2026000000) + 1
        FROM shipments WHERE shipment_number ~ '^EXP[0-9]+$'
    """)).scalar()


# =====================================================================
# LẦN CHẠY ĐẦU: SINH TOÀN BỘ DỮ LIỆU OLTP
# =====================================================================
def generate_data(
    num_customers=DEFAULT_CUSTOMERS,
    num_hubs=DEFAULT_HUBS,
    num_vehicles=DEFAULT_VEHICLES,
    num_couriers=DEFAULT_COURIERS,
    num_shipments=DEFAULT_SHIPMENTS
):
    print("🚀 Bắt đầu khởi tạo dữ liệu giả lập OLTP (kèm dữ liệu bẩn có kiểm soát & kịch bản SCD2)...")
    gen_now = datetime.now()
    base_time = gen_now - timedelta(days=30)

    with get_oltp_conn() as conn:
        existing_shipments = conn.execute(text("SELECT COUNT(*) FROM shipments")).scalar()
        if existing_shipments > 0:
            print(f"ℹ️ OLTP đã có {existing_shipments} vận đơn. Tiến hành mô phỏng cập nhật SCD2 & đơn mới...")
            simulate_scd2_and_incremental_updates(conn)
            conn.commit()
            return

        # 1. Sinh Hubs
        hub_rows = []
        for i in range(1, num_hubs + 1):
            city, districts = random.choice(CITIES)
            district = random.choice(districts)
            hub_level = "Regional Hub" if i <= 6 else "Local Branch"
            hub_rows.append({
                "hub_code": f"HUB_{i:03d}",
                "hub_name": f"Bưu cục {district} - {city} #{i}",
                "hub_level": hub_level,
                "city": city,
                "district": district,
                "capacity": random.randint(2000, 8000) if hub_level == "Regional Hub" else random.randint(500, 2000),
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO hubs (hub_code, hub_name, hub_level, city, district, capacity, created_at, updated_at)
            VALUES (:hub_code, :hub_name, :hub_level, :city, :district, :capacity, :created_at, :updated_at)
        """), hub_rows)

        # 2. Sinh Vehicles
        vehicle_rows = []
        v_types = [("Motorbike", 50), ("Van", 800), ("Truck_2T", 2000), ("Truck_8T", 8000)]
        for i in range(1, num_vehicles + 1):
            v_type, cap = random.choice(v_types)
            vehicle_rows.append({
                "vehicle_code": f"VEH_{i:03d}",
                "vehicle_type": v_type,
                "capacity_kg": cap,
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO vehicles (vehicle_code, vehicle_type, capacity_kg, created_at, updated_at)
            VALUES (:vehicle_code, :vehicle_type, :capacity_kg, :created_at, :updated_at)
        """), vehicle_rows)

        hub_ids = [r[0] for r in conn.execute(text("SELECT hub_id FROM hubs ORDER BY hub_id")).fetchall()]
        vehicle_ids = [r[0] for r in conn.execute(text("SELECT vehicle_id FROM vehicles ORDER BY vehicle_id")).fetchall()]

        # 3. Sinh Customers (Cài cắm 2% số điện thoại sai định dạng)
        customer_rows = []
        for i in range(1, num_customers + 1):
            city, districts = random.choice(CITIES)
            is_bad_phone = (random.random() < 0.02)
            phone = "09" + "".join([str(random.randint(0, 9)) for _ in range(8)])
            if is_bad_phone:
                phone = "BAD_PHONE_###_" + str(i)

            customer_rows.append({
                "customer_code": f"CUST_{i:05d}",
                "customer_name": fake.name(),
                "customer_type": random.choice(["Individual", "Shop", "B2B"]),
                "membership_tier": random.choice(["Bronze", "Silver", "Gold"]),
                "phone_number": phone,
                "city": city,
                "district": random.choice(districts),
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO customers (customer_code, customer_name, customer_type, membership_tier, phone_number, city, district, created_at, updated_at)
            VALUES (:customer_code, :customer_name, :customer_type, :membership_tier, :phone_number, :city, :district, :created_at, :updated_at)
        """), customer_rows)

        # 4. Sinh Couriers
        courier_rows = []
        for i in range(1, num_couriers + 1):
            courier_rows.append({
                "courier_code": f"COUR_{i:04d}",
                "courier_name": fake.name(),
                "hub_id": random.choice(hub_ids),
                "contract_type": random.choice(["Full-time", "Part-time"]),
                "vehicle_id": random.choice(vehicle_ids),
                "effective_date": base_time.date(),
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO couriers (courier_code, courier_name, hub_id, contract_type, vehicle_id, effective_date, created_at, updated_at)
            VALUES (:courier_code, :courier_name, :hub_id, :contract_type, :vehicle_id, :effective_date, :created_at, :updated_at)
        """), courier_rows)

        # 5. Sinh Shipments + Sự kiện + COD + Ticket
        lk = _load_lookups(conn)
        shipment_rows, timeline = _build_shipments(
            lk, _next_shipment_number(conn), num_shipments,
            order_from=base_time, order_to=base_time + timedelta(days=26), gen_now=gen_now
        )
        inserted = _insert_shipments(conn, shipment_rows)
        event_rows, cod_rows, ticket_rows = _build_children(lk, inserted, timeline, gen_now)
        _insert_children(conn, event_rows, cod_rows, ticket_rows)

        conn.commit()
        print(f"✅ Đã sinh thành công {len(shipment_rows)} Shipments, {len(event_rows)} Events, {len(cod_rows)} COD & {len(ticket_rows)} Tickets!")


# =====================================================================
# CÁC LẦN CHẠY SAU: MÔ PHỎNG THAY ĐỔI ĐỂ DEMO INCREMENTAL / SCD
# =====================================================================
def simulate_scd2_and_incremental_updates(conn):
    """
    Mô phỏng thay đổi dữ liệu ở lần chạy thứ 2 trở đi để chứng minh:
    1. Khách hàng thăng hạng Membership Tier (SCD Type 2)
    2. Shipper điều chuyển sang Hub mới, có ngày hiệu lực (SCD Type 2)
    3. Hub đổi tên / tăng công suất (SCD Type 1 + đồng bộ thuộc tính denormalize trong Dim_Courier)
    4. Vận đơn IN_TRANSIT được giao thành công (kèm sự kiện quét) + Quyết toán COD (Accumulating Snapshot)
    5. Vận đơn MỚI phát sinh trong 2 ngày gần nhất (Incremental Insert)
    """
    now_ts = datetime.now() - SAFETY_MARGIN

    # 1. [AUDIT-F6] Chỉ chọn khách CHƯA ở hạng Gold -> 25 khách đều thật sự đổi hạng (trước đây khách Gold "đổi" thành Gold)
    cust_cnt = conn.execute(text("""
        UPDATE customers
        SET membership_tier = CASE WHEN membership_tier = 'Bronze' THEN 'Silver' ELSE 'Gold' END,
            updated_at = :now_ts
        WHERE customer_id IN (
            SELECT customer_id FROM customers WHERE membership_tier <> 'Gold' ORDER BY customer_id LIMIT 25
        )
    """), {"now_ts": now_ts}).rowcount

    # 2. [AUDIT-F6] Chỉ điều chuyển shipper chưa ở Hub đích, ghi rõ ngày hiệu lực của quyết định điều chuyển
    cour_cnt = conn.execute(text("""
        UPDATE couriers
        SET hub_id = (SELECT MAX(hub_id) FROM hubs),
            contract_type = 'Full-time',
            effective_date = CAST(:now_ts AS DATE),
            updated_at = :now_ts
        WHERE courier_id IN (
            SELECT courier_id FROM couriers
            WHERE hub_id <> (SELECT MAX(hub_id) FROM hubs)
            ORDER BY courier_id LIMIT 15
        )
    """), {"now_ts": now_ts}).rowcount

    # 3. SCD1 trên Hub
    conn.execute(text("""
        UPDATE hubs
        SET hub_name = hub_name || ' (Mở rộng)', capacity = capacity + 500, updated_at = :now_ts
        WHERE hub_id = (SELECT MIN(hub_id) FROM hubs) AND POSITION('(Mở rộng)' IN hub_name) = 0
    """), {"now_ts": now_ts})

    lk = _load_lookups(conn)

    # 4. [AUDIT-F6] Đơn IN_TRANSIT -> DELIVERED phải đi kèm sự kiện INBOUND đích + DELIVERED
    #    (trước đây chỉ đổi trạng thái, Fact_Tracking_Events & Snapshot không hề biết đơn đã giao)
    delivered = conn.execute(text("""
        UPDATE shipments s
        SET current_status = 'DELIVERED', actual_delivery_date = :now_ts, updated_at = :now_ts
        WHERE s.shipment_id IN (
            SELECT shipment_id FROM shipments WHERE current_status = 'IN_TRANSIT' ORDER BY shipment_id LIMIT 50
        )
        RETURNING s.shipment_id, s.destination_hub_id,
                  (SELECT MAX(e.event_time) FROM shipment_tracking_events e WHERE e.shipment_id = s.shipment_id) AS last_event_time
    """), {"now_ts": now_ts}).fetchall()

    event_rows = []
    for sh_id, dest_h, last_event_time in delivered:
        c2 = _pick_courier(lk, dest_h)
        v2 = random.choice(lk["vehicle_ids"])
        start = last_event_time or (now_ts - timedelta(hours=3))
        in_dest_dt = start + (now_ts - start) * 0.6
        for ts, ev_type, reason, source in (
            (in_dest_dt, "INBOUND_SCAN", "RS_HUB_IN", "HUB_GATE_SCAN"),
            (now_ts, "DELIVERED", "RS_DELIV_OK", "PDA_SCANNER"),
        ):
            event_rows.append({
                "shipment_id": sh_id, "event_time": ts, "event_type": ev_type,
                "hub_id": dest_h, "courier_id": c2, "vehicle_id": v2,
                "status_reason_id": lk["reasons"][reason], "scan_source": source,
                "created_at": now_ts, "updated_at": now_ts
            })
    _insert_children(conn, event_rows, [], [])

    # [AUDIT-F6] Chỉ quyết toán COD của đơn đã giao thành công (trước đây có thể quyết toán cả đơn hoàn/đang đi)
    cod_cnt = conn.execute(text("""
        UPDATE cod_settlements
        SET settlement_status = 'SETTLED', settled_at = :now_ts, updated_at = :now_ts
        WHERE settlement_id IN (
            SELECT c.settlement_id FROM cod_settlements c
            JOIN shipments s ON s.shipment_id = c.shipment_id
            WHERE c.settlement_status = 'PENDING' AND s.current_status = 'DELIVERED'
            ORDER BY c.settlement_id LIMIT 40
        )
    """), {"now_ts": now_ts}).rowcount

    # 5. [AUDIT-F6] Vận đơn mới thật sự (thông báo cũ có ghi "& đơn mới" nhưng code không sinh)
    gen_now = datetime.now()
    shipment_rows, timeline = _build_shipments(
        lk, _next_shipment_number(conn), SIMULATE_NEW_SHIPMENTS,
        order_from=gen_now - timedelta(hours=48), order_to=gen_now - timedelta(hours=1), gen_now=gen_now,
        audit_ts=now_ts
    )
    inserted = _insert_shipments(conn, shipment_rows)
    new_events, new_cod, new_tickets = _build_children(lk, inserted, timeline, gen_now, audit_ts=now_ts)
    _insert_children(conn, new_events, new_cod, new_tickets)

    print(f"✅ Đã cập nhật {cust_cnt} Customers (đổi hạng), {cour_cnt} Couriers (chuyển Hub), 1 Hub (SCD1), "
          f"{len(delivered)} Shipments giao thành công, {cod_cnt} COD Settlements "
          f"& sinh {len(shipment_rows)} vận đơn mới ({len(new_events)} sự kiện)!")


if __name__ == "__main__":
    generate_data()
```

### File: `scripts/extract_incremental.py`

```python
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from config.db_config import get_oltp_conn, get_dwh_conn

TABLES_TO_EXTRACT = [
    "customers", "hubs", "vehicles", "couriers", "service_types",
    "payment_methods", "status_reasons", "shipments",
    "shipment_tracking_events", "cod_settlements", "customer_tickets"
]

# [AUDIT-A2] Cửa sổ chồng lấn (lookback): đọc lùi N phút so với watermark để không bỏ sót các giao dịch OLTP
# commit muộn (updated_at nhỏ hơn watermark nhưng commit SAU lần extract trước). An toàn vì Silver/Gold đều upsert idempotent.
EXTRACT_LOOKBACK_MINUTES = 10
# [AUDIT-A4] Số batch SUCCESS gần nhất được giữ lại trong stg (ngoài batch hiện tại) để kiểm toán
STG_RETENTION_BATCHES = 3


def run_extract_incremental(batch_id: str):
    print(f"📥 [Batch: {batch_id}] Bắt đầu Incremental Extract từ OLTP sang Staging...")
    extracted_counts = {}

    with get_dwh_conn() as dwh_conn, get_oltp_conn() as oltp_conn:
        # [AUDIT-A5] Chống 2 batch chạy chồng nhau (Airflow + chạy tay run_local_pipeline.py): hai batch cùng ghi
        # Silver/Gold và cùng đẩy watermark sẽ làm hỏng dữ liệu. Advisory lock bảo đảm phép kiểm tra là nguyên tử;
        # khóa được nhả ở lần commit đầu tiên, khi dòng RUNNING của batch này đã hiển thị cho tiến trình khác.
        dwh_conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('express_dwh_pipeline'))"))
        running_batch = dwh_conn.execute(text("""
            SELECT batch_id FROM meta.etl_batch_log
            WHERE batch_id <> :batch_id
              AND status IN ('RUNNING', 'EXTRACTED')
              AND run_start > LOCALTIMESTAMP - INTERVAL '6 hours'
            ORDER BY run_start DESC
            LIMIT 1
        """), {"batch_id": batch_id}).scalar()
        if running_batch:
            dwh_conn.rollback()
            raise RuntimeError(
                f"Batch {running_batch} đang chạy dở. Chờ batch đó xong, hoặc nếu tiến trình đã chết thì đánh dấu: "
                f"UPDATE meta.etl_batch_log SET status = 'FAILED' WHERE batch_id = '{running_batch}';"
            )

        # [AUDIT-A1] Đọc toàn bộ 11 bảng trong CÙNG MỘT snapshot REPEATABLE READ của OLTP.
        # Nếu không, đơn/khách/sự kiện được commit xen giữa lúc đọc bảng cha và bảng con sẽ lệch nhau trong cùng batch.
        src_conn = oltp_conn.execution_options(isolation_level="REPEATABLE READ")
        # [AUDIT-A3] Thời điểm snapshot nguồn (đồng hồ OLTP) -> ghi vào meta.etl_batch_log.source_snapshot_ts,
        # trở thành watermark mới SAU KHI cả pipeline thành công (xem refresh_views.py)
        snapshot_ts = src_conn.execute(text("SELECT LOCALTIMESTAMP")).scalar()

        for table in TABLES_TO_EXTRACT:
            run_start = datetime.now()
            dwh_conn.execute(text("""
                INSERT INTO meta.etl_batch_log (batch_id, table_name, run_start, status)
                VALUES (:batch_id, :table_name, :run_start, 'RUNNING')
                ON CONFLICT (batch_id, table_name) DO UPDATE
                SET run_start = EXCLUDED.run_start, status = 'RUNNING', error_message = NULL
            """), {"batch_id": batch_id, "table_name": table, "run_start": run_start})
            dwh_conn.commit()

            try:
                wm_ts = dwh_conn.execute(text("""
                    SELECT last_extracted_timestamp FROM meta.etl_watermark WHERE table_name = :table_name
                """), {"table_name": table}).scalar() or datetime(1970, 1, 1)
                from_ts = wm_ts - timedelta(minutes=EXTRACT_LOOKBACK_MINUTES)

                query = text(f"SELECT * FROM {table} WHERE updated_at > :from_ts").bindparams(from_ts=from_ts)
                df = pd.read_sql(query, src_conn)
                row_count = len(df)
                extracted_counts[table] = row_count

                # [AUDIT-A4] Idempotent khi chạy lại cùng batch_id + dọn các batch SUCCESS cũ, chỉ giữ N batch gần nhất
                dwh_conn.execute(text(f"""
                    DELETE FROM stg.{table}
                    WHERE batch_id = :batch_id
                       OR batch_id IN (
                            SELECT b.batch_id FROM meta.etl_batch_log b
                            WHERE b.table_name = :table_name AND b.status = 'SUCCESS'
                            ORDER BY b.run_start DESC
                            OFFSET {STG_RETENTION_BATCHES}
                       )
                """), {"batch_id": batch_id, "table_name": table})

                if row_count > 0:
                    df["batch_id"] = batch_id
                    df["extracted_at"] = snapshot_ts
                    df.to_sql(
                        name=table, con=dwh_conn, schema="stg",
                        if_exists="append", index=False, method="multi", chunksize=2000
                    )

                dwh_conn.execute(text("""
                    UPDATE meta.etl_batch_log
                    SET rows_extracted = :rows_extracted, source_snapshot_ts = :snapshot_ts, status = 'EXTRACTED'
                    WHERE batch_id = :batch_id AND table_name = :table_name
                """), {"rows_extracted": row_count, "snapshot_ts": snapshot_ts,
                       "batch_id": batch_id, "table_name": table})
                dwh_conn.commit()
                print(f"   ✔ {table}: Extracted {row_count} rows (updated_at > {from_ts}, snapshot {snapshot_ts})")

            except Exception as exc:
                dwh_conn.rollback()
                dwh_conn.execute(text("""
                    UPDATE meta.etl_batch_log
                    SET run_end = CURRENT_TIMESTAMP, status = 'FAILED', error_message = :err
                    WHERE batch_id = :batch_id AND table_name = :table_name
                """), {"err": str(exc), "batch_id": batch_id, "table_name": table})
                dwh_conn.commit()
                raise

        # Kết thúc transaction đọc (giải phóng snapshot REPEATABLE READ trên OLTP)
        src_conn.rollback()

    return extracted_counts
```

### File: `scripts/data_quality_checks.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn

# Danh mục trạng thái hợp lệ của vận đơn (khớp status_code trong status_reasons, gồm cả DELIVERY_FAILED)
VALID_SHIPMENT_STATUSES = "('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', 'DELIVERY_FAILED', 'DELIVERED', 'RETURNED', 'CANCELLED')"


def run_data_quality_and_silver_load(batch_id: str):
    print(f"🛡️ [Batch: {batch_id}] Chạy Data Quality Gate & Chuẩn hóa dữ liệu vào tầng Silver...")

    with get_dwh_conn() as conn:
        # Xóa bản ghi quarantine cũ nếu chạy lại cùng batch_id (Idempotent)
        conn.execute(text("DELETE FROM meta.data_quarantine WHERE batch_id = :batch_id"), {"batch_id": batch_id})

        # =====================================================================
        # 1. KIỂM TRA RULES TRÊN BẢNG SHIPMENTS (BLOCKER RULES)
        # [AUDIT-B1] record_id = shipment_id (PK nguồn, không bao giờ NULL) -> bước 3.6 loại trừ chính xác,
        #            kể cả khi shipment_number bị NULL (trước đây NULL = NULL không khớp nên lọt xuống Silver và làm vỡ NOT NULL)
        # =====================================================================
        # Rule 1: weight <= 0 (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', CAST(s.shipment_id AS VARCHAR),
                'RULE_WEIGHT_POSITIVE', 'Blocker',
                'Trọng lượng vận đơn nhỏ hơn hoặc bằng 0 kg',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id AND (s.weight IS NULL OR s.weight <= 0);
        """), {"batch_id": batch_id})

        # Rule 2: actual_delivery_date < order_date (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', CAST(s.shipment_id AS VARCHAR),
                'RULE_DELIVERY_AFTER_ORDER', 'Blocker',
                'Thời gian giao hàng thực tế nhỏ hơn thời gian đặt hàng',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND s.actual_delivery_date IS NOT NULL
              AND s.actual_delivery_date < s.order_date;
        """), {"batch_id": batch_id})

        # Rule 3: Thiếu trường bắt buộc / trạng thái không hợp lệ (Blocker)
        # [AUDIT-B2] NULL-safe: "NULL NOT IN (...)" trả về NULL nên trước đây trạng thái NULL lọt qua;
        #            bổ sung DELIVERY_FAILED (có trong status_reasons) và các cột NOT NULL của silver.shipments
        conn.execute(text(f"""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', CAST(s.shipment_id AS VARCHAR),
                'RULE_VALID_SHIPMENT_CORE', 'Blocker',
                'Thiếu số vận đơn/khách gửi/ngày đặt/ngày hẹn giao/cước hoặc trạng thái không nằm trong danh mục chuẩn',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND (
                  s.shipment_number IS NULL
                  OR s.sender_customer_id IS NULL
                  OR s.order_date IS NULL
                  OR s.promised_delivery_date IS NULL
                  OR s.freight_charge IS NULL
                  OR s.current_status IS NULL
                  OR TRIM(s.current_status) NOT IN {VALID_SHIPMENT_STATUSES}
              );
        """), {"batch_id": batch_id})

        # Rule 3c: Mốc thời gian ở TƯƠNG LAI so với thời điểm snapshot nguồn (Blocker)
        # [AUDIT-B9] Đơn "đặt/lấy/giao trong tương lai" là dữ liệu sai; extracted_at = thời điểm snapshot OLTP
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', CAST(s.shipment_id AS VARCHAR),
                'RULE_NO_FUTURE_TIMESTAMP', 'Blocker',
                'Ngày đặt / lấy hàng / giao hàng nằm sau thời điểm trích xuất dữ liệu nguồn',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND GREATEST(s.order_date, s.pickup_date, s.actual_delivery_date) > s.extracted_at + INTERVAL '5 minutes';
        """), {"batch_id": batch_id})

        # Rule 3d: Ngày đặt hàng (mới) muộn hơn sự kiện đã có ở Silver (Blocker)
        # [AUDIT-B10] Rule 4 chỉ kiểm sự kiện MỚI so với vận đơn; rule này kiểm chiều ngược lại khi vận đơn đổi order_date
        #   hoặc vận đơn cha đến muộn sau sự kiện -> giữ nguyên phiên bản hợp lệ trước đó ở Silver
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', CAST(s.shipment_id AS VARCHAR),
                'RULE_ORDER_AFTER_EVENTS', 'Blocker',
                'Ngày đặt hàng muộn hơn sự kiện hành trình đã ghi nhận của chính vận đơn',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND EXISTS (
                  SELECT 1 FROM silver.shipment_tracking_events e
                  WHERE e.shipment_id = s.shipment_id AND e.event_time < s.order_date
              );
        """), {"batch_id": batch_id})

        # Rule 3b: Thiếu khóa ngoại phụ (Warning -> Map Unknown -1 ở Gold)
        # [AUDIT-B3] OLTP cho phép NULL các cột này; trước đây Silver khai báo NOT NULL nên 1 dòng NULL làm sập cả batch
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', CAST(s.shipment_id AS VARCHAR),
                'RULE_MISSING_SHIPMENT_FK', 'Warning',
                'Thiếu người nhận/hub đi/hub đến/dịch vụ/phương thức thanh toán -> Sẽ map vào dòng Unknown (SK = -1)',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND (
                  s.receiver_customer_id IS NULL OR s.origin_hub_id IS NULL OR s.destination_hub_id IS NULL
                  OR s.service_type_id IS NULL OR s.payment_method_id IS NULL
              );
        """), {"batch_id": batch_id})

        # =====================================================================
        # 2. KIỂM TRA RULES TRÊN BẢNG TRACKING_EVENTS & CUSTOMERS (XUYÊN BATCH)
        # =====================================================================
        # Rule 4: event_time < order_date (Blocker - Đối chiếu cả stg.shipments lẫn silver.shipments)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_EVENT_TIME_VALID', 'Blocker',
                'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            LEFT JOIN stg.shipments s_stg
              ON e.shipment_id = s_stg.shipment_id AND s_stg.batch_id = :batch_id
            LEFT JOIN silver.shipments s_sil
              ON e.shipment_id = s_sil.shipment_id
            WHERE e.batch_id = :batch_id
              AND e.event_time < COALESCE(s_stg.order_date, s_sil.order_date);
        """), {"batch_id": batch_id})

        # Rule 4b: Thiếu trường bắt buộc của sự kiện (Blocker)
        # [AUDIT-B4] Bảo vệ các cột NOT NULL của silver.shipment_tracking_events
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_EVENT_REQUIRED_FIELDS', 'Blocker',
                'Sự kiện thiếu shipment_id, event_time hoặc event_type',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            WHERE e.batch_id = :batch_id
              AND (e.shipment_id IS NULL OR e.event_time IS NULL OR e.event_type IS NULL);
        """), {"batch_id": batch_id})

        # Rule 4c: Sự kiện quét ở TƯƠNG LAI so với thời điểm snapshot nguồn (Blocker) - [AUDIT-B9]
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_NO_FUTURE_TIMESTAMP', 'Blocker',
                'Thời gian quét sự kiện nằm sau thời điểm trích xuất dữ liệu nguồn',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            WHERE e.batch_id = :batch_id
              AND e.event_time > e.extracted_at + INTERVAL '5 minutes';
        """), {"batch_id": batch_id})

        # Rule 5: Thiếu courier_id hoặc hub_id trong tracking_events (Warning -> Map Unknown -1)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_MISSING_HUB_OR_COURIER', 'Warning',
                'Sự kiện thiếu hub_id hoặc courier_id -> Sẽ map vào dòng Unknown (SK = -1)',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            WHERE e.batch_id = :batch_id AND (e.hub_id IS NULL OR e.courier_id IS NULL);
        """), {"batch_id": batch_id})

        # Rule 6: Số điện thoại sai định dạng ở bảng customers (Warning)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'customers', customer_code,
                'RULE_INVALID_PHONE_FORMAT', 'Warning',
                'Số điện thoại sai định dạng -> Silver loại ký tự lạ; nếu vẫn không hợp lệ thì gán NULL (không bịa số)',
                to_jsonb(c)
            FROM stg.customers c
            WHERE c.batch_id = :batch_id AND c.phone_number !~ '^[0-9]{9,12}$';
        """), {"batch_id": batch_id})

        # =====================================================================
        # 3. NẠP DỮ LIỆU HỢP LỆ TỪ STG SANG SILVER (UPSERT + TÍNH ROW_HASH CHO SCD2)
        # [AUDIT-B5] Mọi upsert:
        #   - Cập nhật ĐỦ cột nghiệp vụ (kể cả mã NK) để Silver luôn phản ánh trạng thái mới nhất của nguồn
        #   - Chỉ ghi đè khi bản ghi đến KHÔNG cũ hơn bản đang có (tgt.updated_at <= EXCLUDED.updated_at)
        #     -> chống "quay ngược" dữ liệu khi chạy lại batch cũ / cửa sổ lookback
        # =====================================================================
        # 3.1. Silver Customers
        # [AUDIT-B6] Không bịa số '0000000000' và không giữ chuỗi số rác: chỉ giữ số hợp lệ 9-12 chữ số, còn lại NULL
        conn.execute(text("""
            INSERT INTO silver.customers AS tgt (
                customer_id, customer_code, customer_name, customer_type,
                membership_tier, phone_number, city, district, row_hash, updated_at, batch_id
            )
            SELECT DISTINCT ON (customer_id)
                customer_id,
                TRIM(customer_code),
                TRIM(customer_name),
                TRIM(customer_type),
                TRIM(membership_tier),
                CASE
                    WHEN REGEXP_REPLACE(COALESCE(phone_number, ''), '[^0-9]', '', 'g') ~ '^[0-9]{9,12}$'
                    THEN REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g')
                    ELSE NULL
                END AS phone_number,
                TRIM(city),
                TRIM(district),
                MD5(
                    COALESCE(TRIM(customer_name), '') || '|' ||
                    COALESCE(TRIM(customer_type), '') || '|' ||
                    COALESCE(TRIM(membership_tier), '') || '|' ||
                    COALESCE(TRIM(city), '') || '|' ||
                    COALESCE(TRIM(district), '')
                ) AS row_hash,
                updated_at,
                :batch_id
            FROM stg.customers
            WHERE batch_id = :batch_id
            ORDER BY customer_id, updated_at DESC
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_code = EXCLUDED.customer_code,
                customer_name = EXCLUDED.customer_name,
                customer_type = EXCLUDED.customer_type,
                membership_tier = EXCLUDED.membership_tier,
                phone_number = EXCLUDED.phone_number,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                row_hash = EXCLUDED.row_hash,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.2. Silver Hubs
        conn.execute(text("""
            INSERT INTO silver.hubs AS tgt (hub_id, hub_code, hub_name, hub_level, city, district, capacity, updated_at, batch_id)
            SELECT DISTINCT ON (hub_id)
                hub_id, TRIM(hub_code), TRIM(hub_name), TRIM(hub_level),
                TRIM(city), TRIM(district), capacity, updated_at, :batch_id
            FROM stg.hubs
            WHERE batch_id = :batch_id
            ORDER BY hub_id, updated_at DESC
            ON CONFLICT (hub_id) DO UPDATE SET
                hub_code = EXCLUDED.hub_code,
                hub_name = EXCLUDED.hub_name,
                hub_level = EXCLUDED.hub_level,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                capacity = EXCLUDED.capacity,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.3. Silver Vehicles
        conn.execute(text("""
            INSERT INTO silver.vehicles AS tgt (vehicle_id, vehicle_code, vehicle_type, capacity_kg, updated_at, batch_id)
            SELECT DISTINCT ON (vehicle_id)
                vehicle_id, TRIM(vehicle_code), TRIM(vehicle_type), capacity_kg, updated_at, :batch_id
            FROM stg.vehicles
            WHERE batch_id = :batch_id
            ORDER BY vehicle_id, updated_at DESC
            ON CONFLICT (vehicle_id) DO UPDATE SET
                vehicle_code = EXCLUDED.vehicle_code,
                vehicle_type = EXCLUDED.vehicle_type,
                capacity_kg = EXCLUDED.capacity_kg,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.4. Silver Couriers
        conn.execute(text("""
            INSERT INTO silver.couriers AS tgt (
                courier_id, courier_code, courier_name, hub_id,
                contract_type, vehicle_id, effective_date, row_hash, updated_at, batch_id
            )
            SELECT DISTINCT ON (courier_id)
                courier_id,
                TRIM(courier_code),
                TRIM(courier_name),
                hub_id,
                TRIM(contract_type),
                vehicle_id,
                effective_date,
                MD5(
                    COALESCE(TRIM(courier_name), '') || '|' ||
                    COALESCE(CAST(hub_id AS VARCHAR), '-1') || '|' ||
                    COALESCE(TRIM(contract_type), '') || '|' ||
                    COALESCE(CAST(vehicle_id AS VARCHAR), '-1')
                ) AS row_hash,
                updated_at,
                :batch_id
            FROM stg.couriers
            WHERE batch_id = :batch_id
            ORDER BY courier_id, updated_at DESC
            ON CONFLICT (courier_id) DO UPDATE SET
                courier_code = EXCLUDED.courier_code,
                courier_name = EXCLUDED.courier_name,
                hub_id = EXCLUDED.hub_id,
                contract_type = EXCLUDED.contract_type,
                vehicle_id = EXCLUDED.vehicle_id,
                effective_date = EXCLUDED.effective_date,
                row_hash = EXCLUDED.row_hash,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.5. Silver Service Types, Payment Methods, Status Reasons
        conn.execute(text("""
            INSERT INTO silver.service_types AS tgt (service_type_id, service_code, service_name, sla_hours, is_express, updated_at, batch_id)
            SELECT DISTINCT ON (service_type_id)
                service_type_id, TRIM(service_code), TRIM(service_name), sla_hours, is_express, updated_at, :batch_id
            FROM stg.service_types WHERE batch_id = :batch_id
            ORDER BY service_type_id, updated_at DESC
            ON CONFLICT (service_type_id) DO UPDATE SET
                service_code = EXCLUDED.service_code, service_name = EXCLUDED.service_name,
                sla_hours = EXCLUDED.sla_hours, is_express = EXCLUDED.is_express,
                updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;

            INSERT INTO silver.payment_methods AS tgt (payment_method_id, payment_method_code, payment_method_name, payer_type, has_cod, updated_at, batch_id)
            SELECT DISTINCT ON (payment_method_id)
                payment_method_id, TRIM(payment_method_code), TRIM(payment_method_name), TRIM(payer_type), has_cod, updated_at, :batch_id
            FROM stg.payment_methods WHERE batch_id = :batch_id
            ORDER BY payment_method_id, updated_at DESC
            ON CONFLICT (payment_method_id) DO UPDATE SET
                payment_method_code = EXCLUDED.payment_method_code, payment_method_name = EXCLUDED.payment_method_name,
                payer_type = EXCLUDED.payer_type, has_cod = EXCLUDED.has_cod,
                updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;

            INSERT INTO silver.status_reasons AS tgt (status_reason_id, status_code, status_name, reason_code, reason_name, event_category, updated_at, batch_id)
            SELECT DISTINCT ON (status_reason_id)
                status_reason_id, TRIM(status_code), TRIM(status_name), TRIM(reason_code), TRIM(reason_name), TRIM(event_category), updated_at, :batch_id
            FROM stg.status_reasons WHERE batch_id = :batch_id
            ORDER BY status_reason_id, updated_at DESC
            ON CONFLICT (status_reason_id) DO UPDATE SET
                status_code = EXCLUDED.status_code, status_name = EXCLUDED.status_name,
                reason_code = EXCLUDED.reason_code, reason_name = EXCLUDED.reason_name,
                event_category = EXCLUDED.event_category,
                updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.6. Silver Shipments (Loại bỏ tất cả các bản ghi dính lỗi Blocker trong Quarantine)
        conn.execute(text("""
            INSERT INTO silver.shipments AS tgt (
                shipment_id, shipment_number, sender_customer_id, receiver_customer_id,
                origin_hub_id, destination_hub_id, service_type_id, payment_method_id,
                weight, distance_km, freight_charge, cod_amount,
                order_date, pickup_date, promised_delivery_date, actual_delivery_date,
                current_status, updated_at, batch_id
            )
            SELECT DISTINCT ON (s.shipment_id)
                s.shipment_id, TRIM(s.shipment_number), s.sender_customer_id, s.receiver_customer_id,
                s.origin_hub_id, s.destination_hub_id, s.service_type_id, s.payment_method_id,
                s.weight, COALESCE(s.distance_km, 10.0), s.freight_charge, COALESCE(s.cod_amount, 0),
                s.order_date, s.pickup_date, s.promised_delivery_date, s.actual_delivery_date,
                TRIM(s.current_status), s.updated_at, :batch_id
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND NOT EXISTS (
                  SELECT 1 FROM meta.data_quarantine q
                  WHERE q.batch_id = :batch_id
                    AND q.source_table = 'shipments'
                    AND q.severity = 'Blocker'
                    AND q.record_id = CAST(s.shipment_id AS VARCHAR)
              )
            ORDER BY s.shipment_id, s.updated_at DESC
            ON CONFLICT (shipment_id) DO UPDATE SET
                shipment_number = EXCLUDED.shipment_number,
                sender_customer_id = EXCLUDED.sender_customer_id,
                receiver_customer_id = EXCLUDED.receiver_customer_id,
                origin_hub_id = EXCLUDED.origin_hub_id,
                destination_hub_id = EXCLUDED.destination_hub_id,
                service_type_id = EXCLUDED.service_type_id,
                payment_method_id = EXCLUDED.payment_method_id,
                weight = EXCLUDED.weight,
                distance_km = EXCLUDED.distance_km,
                freight_charge = EXCLUDED.freight_charge,
                cod_amount = EXCLUDED.cod_amount,
                order_date = EXCLUDED.order_date,
                pickup_date = EXCLUDED.pickup_date,
                promised_delivery_date = EXCLUDED.promised_delivery_date,
                actual_delivery_date = EXCLUDED.actual_delivery_date,
                current_status = EXCLUDED.current_status,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.6b. [AUDIT-B7] Bản ghi con (event/COD/ticket) thuộc vận đơn CHƯA hợp lệ ở Silver (bị Blocker hoặc đến muộn):
        #   Trước đây JOIN silver.shipments ở bước 3.7-3.9 làm chúng biến mất KHÔNG dấu vết, watermark vẫn tiến
        #   -> khi vận đơn cha được sửa ở nguồn thì các bản ghi con không bao giờ được extract lại (mất vĩnh viễn).
        #   Nay: ghi Warning để truy vết + VẪN nạp vào Silver; Gold chỉ đẩy lên khi vận đơn cha hợp lệ (late-arriving).
        for child_table, child_pk in (
            ("shipment_tracking_events", "event_id"),
            ("cod_settlements", "settlement_id"),
            ("customer_tickets", "ticket_id"),
        ):
            conn.execute(text(f"""
                INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
                SELECT
                    :batch_id, '{child_table}', CAST(c.{child_pk} AS VARCHAR),
                    'RULE_ORPHAN_SHIPMENT', 'Warning',
                    'Vận đơn cha chưa có/chưa hợp lệ ở Silver -> Giữ ở Silver, tự lên Gold khi vận đơn cha hợp lệ',
                    to_jsonb(c)
                FROM stg.{child_table} c
                WHERE c.batch_id = :batch_id
                  AND c.shipment_id IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM silver.shipments ss WHERE ss.shipment_id = c.shipment_id);
            """), {"batch_id": batch_id})

        # 3.7. Silver Tracking Events (không còn INNER JOIN silver.shipments - xem AUDIT-B7)
        conn.execute(text("""
            INSERT INTO silver.shipment_tracking_events AS tgt (
                event_id, shipment_id, event_time, event_type,
                hub_id, courier_id, vehicle_id, status_reason_id,
                scan_source, updated_at, batch_id
            )
            SELECT DISTINCT ON (e.event_id)
                e.event_id, e.shipment_id, e.event_time, TRIM(e.event_type),
                e.hub_id, e.courier_id, e.vehicle_id, e.status_reason_id,
                TRIM(e.scan_source), e.updated_at, :batch_id
            FROM stg.shipment_tracking_events e
            WHERE e.batch_id = :batch_id
              AND NOT EXISTS (
                  SELECT 1 FROM meta.data_quarantine q
                  WHERE q.batch_id = :batch_id
                    AND q.source_table = 'shipment_tracking_events'
                    AND q.severity = 'Blocker'
                    AND q.record_id = CAST(e.event_id AS VARCHAR)
              )
            ORDER BY e.event_id, e.updated_at DESC
            ON CONFLICT (event_id) DO UPDATE SET
                shipment_id = EXCLUDED.shipment_id,
                event_time = EXCLUDED.event_time,
                event_type = EXCLUDED.event_type,
                hub_id = EXCLUDED.hub_id,
                courier_id = EXCLUDED.courier_id,
                vehicle_id = EXCLUDED.vehicle_id,
                status_reason_id = EXCLUDED.status_reason_id,
                scan_source = EXCLUDED.scan_source,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.8. Silver COD Settlements (không còn INNER JOIN silver.shipments - xem AUDIT-B7)
        conn.execute(text("""
            INSERT INTO silver.cod_settlements AS tgt (
                settlement_id, shipment_id, cod_amount, settlement_status, settled_at, updated_at, batch_id
            )
            SELECT DISTINCT ON (c.settlement_id)
                c.settlement_id, c.shipment_id, c.cod_amount, TRIM(c.settlement_status), c.settled_at, c.updated_at, :batch_id
            FROM stg.cod_settlements c
            WHERE c.batch_id = :batch_id
            ORDER BY c.settlement_id, c.updated_at DESC
            ON CONFLICT (settlement_id) DO UPDATE SET
                shipment_id = EXCLUDED.shipment_id,
                cod_amount = EXCLUDED.cod_amount,
                settlement_status = EXCLUDED.settlement_status,
                settled_at = EXCLUDED.settled_at,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # 3.9. Silver Customer Tickets (không còn INNER JOIN silver.shipments - xem AUDIT-B7)
        conn.execute(text("""
            INSERT INTO silver.customer_tickets AS tgt (
                ticket_id, shipment_id, customer_id, issue_type, ticket_status, created_at, updated_at, batch_id
            )
            SELECT DISTINCT ON (t.ticket_id)
                t.ticket_id, t.shipment_id, t.customer_id, TRIM(t.issue_type), TRIM(t.ticket_status), t.created_at, t.updated_at, :batch_id
            FROM stg.customer_tickets t
            WHERE t.batch_id = :batch_id
            ORDER BY t.ticket_id, t.updated_at DESC
            ON CONFLICT (ticket_id) DO UPDATE SET
                shipment_id = EXCLUDED.shipment_id,
                customer_id = EXCLUDED.customer_id,
                issue_type = EXCLUDED.issue_type,
                ticket_status = EXCLUDED.ticket_status,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id
            WHERE tgt.updated_at <= EXCLUDED.updated_at;
        """), {"batch_id": batch_id})

        # Cập nhật số dòng bị từ chối (Blocker) vào meta.etl_batch_log
        # [AUDIT-B8] COUNT(DISTINCT record_id): 1 bản ghi dính nhiều rule Blocker chỉ tính là 1 dòng bị từ chối
        conn.execute(text("""
            UPDATE meta.etl_batch_log b
            SET rows_rejected = (
                SELECT COUNT(DISTINCT q.record_id) FROM meta.data_quarantine q
                WHERE q.batch_id = b.batch_id AND q.source_table = b.table_name AND q.severity = 'Blocker'
            )
            WHERE b.batch_id = :batch_id;
        """), {"batch_id": batch_id})

        blocker_cnt = conn.execute(text("""
            SELECT COUNT(*) FROM meta.data_quarantine WHERE batch_id = :batch_id AND severity = 'Blocker'
        """), {"batch_id": batch_id}).scalar()
        warning_cnt = conn.execute(text("""
            SELECT COUNT(*) FROM meta.data_quarantine WHERE batch_id = :batch_id AND severity = 'Warning'
        """), {"batch_id": batch_id}).scalar()

        conn.commit()
        print(f"✅ Data Quality Gate hoàn tất: {blocker_cnt} bản ghi Blocker bị cách ly, {warning_cnt} cảnh báo Warning đã được xử lý.")
```

### File: `scripts/load_dimensions.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_load_dimensions(batch_id: str):
    print(f"🏛️ [Batch: {batch_id}] Đang nạp 8 bảng Dimensions (SCD Type 1 & SCD Type 2)...")

    with get_dwh_conn() as conn:
        # 1. Nạp Dim_Date (Lịch từ 2024 tới MAX(2028-12-31, hôm nay + 2 năm))
        # [AUDIT-D4] Cận trên động để lịch không "hết hạn"; ngày vẫn nằm ngoài lịch được gold.fn_date_key() map về -1
        conn.execute(text("""
            INSERT INTO gold.dim_date (
                date_key, full_date, year, quarter, month, month_name,
                week, day_of_week, day_of_month, is_weekend, is_peak_season
            )
            SELECT
                TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
                d::DATE AS full_date,
                EXTRACT(YEAR FROM d)::INT AS year,
                EXTRACT(QUARTER FROM d)::INT AS quarter,
                EXTRACT(MONTH FROM d)::INT AS month,
                TRIM(TO_CHAR(d, 'Month')) AS month_name,
                EXTRACT(WEEK FROM d)::INT AS week,
                EXTRACT(ISODOW FROM d)::INT AS day_of_week,
                EXTRACT(DAY FROM d)::INT AS day_of_month,
                CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
                CASE WHEN EXTRACT(MONTH FROM d) IN (1, 11, 12) THEN TRUE ELSE FALSE END AS is_peak_season
            FROM GENERATE_SERIES(
                '2024-01-01'::DATE,
                GREATEST('2028-12-31'::DATE, (CURRENT_DATE + INTERVAL '2 years')::DATE),
                '1 day'::INTERVAL
            ) AS d
            ON CONFLICT (date_key) DO NOTHING;
        """))

        # 2. Nạp Dim_Hub (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_hub (hub_nk, hub_name, hub_level, city, district, capacity, is_current)
            SELECT hub_code, hub_name, hub_level, city, district, capacity, TRUE
            FROM silver.hubs
            WHERE batch_id = :batch_id
            ON CONFLICT (hub_nk) DO UPDATE SET
                hub_name = EXCLUDED.hub_name,
                hub_level = EXCLUDED.hub_level,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                capacity = EXCLUDED.capacity;
        """), {"batch_id": batch_id})

        # 3. Nạp Dim_Vehicle (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_vehicle (vehicle_nk, vehicle_type, capacity_kg)
            SELECT vehicle_code, vehicle_type, capacity_kg
            FROM silver.vehicles
            WHERE batch_id = :batch_id
            ON CONFLICT (vehicle_nk) DO UPDATE SET
                vehicle_type = EXCLUDED.vehicle_type,
                capacity_kg = EXCLUDED.capacity_kg;
        """), {"batch_id": batch_id})

        # 4. Nạp Dim_Service_Type (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_service_type (service_type_nk, service_name, sla_hours, is_express)
            SELECT service_code, service_name, sla_hours, is_express
            FROM silver.service_types
            WHERE batch_id = :batch_id
            ON CONFLICT (service_type_nk) DO UPDATE SET
                service_name = EXCLUDED.service_name,
                sla_hours = EXCLUDED.sla_hours,
                is_express = EXCLUDED.is_express;
        """), {"batch_id": batch_id})

        # 5. Nạp Dim_Status_Reason (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_status_reason (status_code, status_name, reason_code, reason_name, event_category)
            SELECT status_code, status_name, reason_code, reason_name, event_category
            FROM silver.status_reasons
            WHERE batch_id = :batch_id
            ON CONFLICT (reason_code) DO UPDATE SET
                status_code = EXCLUDED.status_code,
                status_name = EXCLUDED.status_name,
                reason_name = EXCLUDED.reason_name,
                event_category = EXCLUDED.event_category;
        """), {"batch_id": batch_id})

        # 6. Nạp Dim_Payment_Method (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_payment_method (payment_method_nk, payment_method_name, payer_type, has_cod)
            SELECT payment_method_code, payment_method_name, payer_type, has_cod
            FROM silver.payment_methods
            WHERE batch_id = :batch_id
            ON CONFLICT (payment_method_nk) DO UPDATE SET
                payment_method_name = EXCLUDED.payment_method_name,
                payer_type = EXCLUDED.payer_type,
                has_cod = EXCLUDED.has_cod;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 7. NẠP DIM_CUSTOMER (SCD TYPE 2 BẰNG ROW_HASH)
        # =====================================================================
        conn.execute(text("""
            UPDATE gold.dim_customer d
            SET is_current = FALSE,
                valid_to = s.updated_at
            FROM silver.customers s
            WHERE s.batch_id = :batch_id
              AND d.customer_nk = s.customer_code
              AND d.is_current = TRUE
              AND d.row_hash <> s.row_hash;
        """), {"batch_id": batch_id})

        conn.execute(text("""
            INSERT INTO gold.dim_customer (
                customer_nk, customer_name, customer_type, membership_tier,
                city, district, valid_from, valid_to, is_current, row_hash
            )
            SELECT
                s.customer_code,
                s.customer_name,
                s.customer_type,
                s.membership_tier,
                s.city,
                s.district,
                CASE
                    WHEN EXISTS (SELECT 1 FROM gold.dim_customer prev WHERE prev.customer_nk = s.customer_code)
                    THEN s.updated_at
                    ELSE TIMESTAMP '1970-01-01 00:00:00'
                END AS valid_from,
                TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
                TRUE AS is_current,
                s.row_hash
            FROM silver.customers s
            LEFT JOIN gold.dim_customer d
              ON d.customer_nk = s.customer_code AND d.is_current = TRUE
            WHERE s.batch_id = :batch_id
              AND d.customer_sk IS NULL;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 8. NẠP DIM_COURIER (SCD TYPE 2 DENORMALIZED HUB & VEHICLE)
        # [AUDIT-C2] Mốc chuyển phiên bản = ngày hiệu lực nghiệp vụ (couriers.effective_date) nếu muộn hơn
        #   valid_from của phiên bản hiện tại, ngược lại dùng updated_at. Phiên bản cũ được đóng và phiên bản mới
        #   được mở tại CÙNG một mốc trong cùng một câu lệnh (data-modifying CTE) -> không hở / chồng khoảng hiệu lực.
        #   Fact đã nạp trước đó nằm ngoài khoảng mới sẽ được re-key ở bước 0 của load_facts.py [AUDIT-D8].
        # =====================================================================
        # 8a. Courier đã có và thay đổi thuộc tính SCD2
        conn.execute(text("""
            WITH changed AS (
                SELECT
                    d.courier_sk AS old_sk,
                    s.courier_code, s.courier_name, s.hub_id, s.contract_type, s.vehicle_id, s.row_hash,
                    CASE
                        WHEN s.effective_date IS NOT NULL AND s.effective_date::TIMESTAMP > d.valid_from
                        THEN s.effective_date::TIMESTAMP
                        ELSE s.updated_at
                    END AS boundary_ts
                FROM silver.couriers s
                JOIN gold.dim_courier d
                  ON d.courier_nk = s.courier_code AND d.is_current = TRUE
                WHERE s.batch_id = :batch_id
                  AND d.row_hash <> s.row_hash
            ),
            expired AS (
                UPDATE gold.dim_courier d
                SET is_current = FALSE,
                    valid_to = c.boundary_ts
                FROM changed c
                WHERE d.courier_sk = c.old_sk
                RETURNING d.courier_sk
            )
            INSERT INTO gold.dim_courier (
                courier_nk, courier_name,
                hub_sk, assigned_hub_code, assigned_hub_name, assigned_hub_city,
                contract_type,
                vehicle_sk, assigned_vehicle_type,
                valid_from, valid_to, is_current, row_hash
            )
            SELECT
                c.courier_code,
                c.courier_name,
                COALESCE(dh.hub_sk, -1),
                COALESCE(dh.hub_nk, 'UNKNOWN'),
                COALESCE(dh.hub_name, 'Unknown Hub'),
                COALESCE(dh.city, 'Unknown'),
                c.contract_type,
                COALESCE(dv.vehicle_sk, -1),
                COALESCE(dv.vehicle_type, 'Unknown Vehicle'),
                c.boundary_ts,
                TIMESTAMP '9999-12-31 23:59:59',
                TRUE,
                c.row_hash
            FROM changed c
            LEFT JOIN silver.hubs sh ON c.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.vehicles sv ON c.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk;
        """), {"batch_id": batch_id})

        # 8b. Courier mới hoàn toàn: phiên bản đầu tiên hiệu lực từ 1970 để mọi Fact lịch sử đều join được
        conn.execute(text("""
            INSERT INTO gold.dim_courier (
                courier_nk, courier_name,
                hub_sk, assigned_hub_code, assigned_hub_name, assigned_hub_city,
                contract_type,
                vehicle_sk, assigned_vehicle_type,
                valid_from, valid_to, is_current, row_hash
            )
            SELECT
                s.courier_code,
                s.courier_name,
                COALESCE(dh.hub_sk, -1) AS hub_sk,
                COALESCE(dh.hub_nk, 'UNKNOWN') AS assigned_hub_code,
                COALESCE(dh.hub_name, 'Unknown Hub') AS assigned_hub_name,
                COALESCE(dh.city, 'Unknown') AS assigned_hub_city,
                s.contract_type,
                COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
                COALESCE(dv.vehicle_type, 'Unknown Vehicle') AS assigned_vehicle_type,
                TIMESTAMP '1970-01-01 00:00:00' AS valid_from,
                TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
                TRUE AS is_current,
                s.row_hash
            FROM silver.couriers s
            LEFT JOIN silver.hubs sh ON s.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.vehicles sv ON s.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk
            WHERE s.batch_id = :batch_id
              AND NOT EXISTS (SELECT 1 FROM gold.dim_courier d WHERE d.courier_nk = s.courier_code);
        """), {"batch_id": batch_id})

        # =====================================================================
        # 9. [AUDIT-C1] ĐỒNG BỘ THUỘC TÍNH HUB/VEHICLE ĐÃ DENORMALIZE TRONG DIM_COURIER
        # Dim_Hub / Dim_Vehicle là SCD1 (sửa đè). Bản sao denormalize trong Dim_Courier phải sửa đè theo,
        # nếu không đổi tên/thành phố Hub sẽ làm Dim_Courier lệch vĩnh viễn với Dim_Hub (row_hash không chứa các cột này).
        # Lịch sử SCD2 "courier thuộc hub nào" vẫn giữ nguyên qua hub_sk của từng phiên bản.
        # =====================================================================
        conn.execute(text("""
            UPDATE gold.dim_courier c
            SET assigned_hub_code = h.hub_nk,
                assigned_hub_name = h.hub_name,
                assigned_hub_city = COALESCE(h.city, 'Unknown')
            FROM gold.dim_hub h
            WHERE c.hub_sk = h.hub_sk
              AND c.courier_sk <> -1
              AND (c.assigned_hub_code, c.assigned_hub_name, c.assigned_hub_city)
                  IS DISTINCT FROM (h.hub_nk, h.hub_name, COALESCE(h.city, 'Unknown'));

            UPDATE gold.dim_courier c
            SET assigned_vehicle_type = v.vehicle_type
            FROM gold.dim_vehicle v
            WHERE c.vehicle_sk = v.vehicle_sk
              AND c.courier_sk <> -1
              AND c.assigned_vehicle_type IS DISTINCT FROM v.vehicle_type;
        """))

        conn.commit()
        print("✅ Đã nạp hoàn tất 8 bảng Dimensions!")
```

### File: `scripts/load_facts.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_load_facts(batch_id: str):
    print(f"📊 [Batch: {batch_id}] Đang nạp 3 bảng Facts (SCD2 Point-in-Time Join & Idempotent Upsert)...")

    with get_dwh_conn() as conn:
        # =====================================================================
        # 0. [AUDIT-D8] RE-KEY FACT KHI PHIÊN BẢN SCD2 BỊ "THU HẸP" (thay đổi chiều đến muộn / có ngày hiệu lực lùi)
        # Khi một phiên bản bị đóng (valid_to được gán), các Fact đã nạp trước đó có thời điểm nằm SAU valid_to
        # đang trỏ sai phiên bản -> chuyển sang phiên bản bao trùm đúng thời điểm đó.
        # Chỉ phiên bản đã đóng (is_current = FALSE) mới có thể chứa Fact nằm ngoài khoảng hiệu lực.
        # =====================================================================
        conn.execute(text("""
            UPDATE gold.fact_tracking_events f
            SET courier_sk = c_new.courier_sk, batch_id = :batch_id, loaded_at = CURRENT_TIMESTAMP
            FROM gold.dim_courier c_old
            JOIN gold.dim_courier c_new ON c_new.courier_nk = c_old.courier_nk
            WHERE f.courier_sk = c_old.courier_sk
              AND c_old.is_current = FALSE
              AND NOT (f.event_timestamp >= c_old.valid_from AND f.event_timestamp < c_old.valid_to)
              AND f.event_timestamp >= c_new.valid_from AND f.event_timestamp < c_new.valid_to;

            UPDATE gold.fact_shipment_lifecycle f
            SET sender_customer_sk = c_new.customer_sk, batch_id = :batch_id, updated_at = CURRENT_TIMESTAMP
            FROM gold.dim_customer c_old
            JOIN gold.dim_customer c_new ON c_new.customer_nk = c_old.customer_nk
            WHERE f.sender_customer_sk = c_old.customer_sk
              AND c_old.is_current = FALSE
              AND NOT (f.order_timestamp >= c_old.valid_from AND f.order_timestamp < c_old.valid_to)
              AND f.order_timestamp >= c_new.valid_from AND f.order_timestamp < c_new.valid_to;

            UPDATE gold.fact_shipment_lifecycle f
            SET receiver_customer_sk = c_new.customer_sk, batch_id = :batch_id, updated_at = CURRENT_TIMESTAMP
            FROM gold.dim_customer c_old
            JOIN gold.dim_customer c_new ON c_new.customer_nk = c_old.customer_nk
            WHERE f.receiver_customer_sk = c_old.customer_sk
              AND c_old.is_current = FALSE
              AND NOT (f.order_timestamp >= c_old.valid_from AND f.order_timestamp < c_old.valid_to)
              AND f.order_timestamp >= c_new.valid_from AND f.order_timestamp < c_new.valid_to;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 1. NẠP FACT_SHIPMENT_LIFECYCLE (ACCUMULATING SNAPSHOT - UPSERT)
        #   - [AUDIT-D1] current_status_sk lấy theo shipments.current_status (nguồn sự thật);
        #     lý do (reason) của sự kiện mới nhất chỉ được dùng khi status_code của nó KHỚP trạng thái vận đơn
        #   - [AUDIT-D2] Ngày hoàn tất / lead time / SLA chỉ tính cho đơn DELIVERED (đơn RETURNED dùng return_date_key)
        #   - [AUDIT-D3] ON CONFLICT cập nhật ĐỦ khóa chiều + measure + date key (accumulating snapshot phải "revisit" cả dòng)
        #   - [AUDIT-D4] Date key qua gold.fn_date_key(): ngày ngoài lịch Dim_Date -> -1 thay vì vỡ FK
        #   - Tích hợp dữ liệu COD Settlements và Customer Tickets, lưu cả Date Keys và Milestone Timestamps
        # =====================================================================
        conn.execute(text("""
            WITH impacted_shipments AS (
                SELECT shipment_id FROM silver.shipments WHERE batch_id = :batch_id
                UNION
                SELECT DISTINCT shipment_id FROM silver.shipment_tracking_events WHERE batch_id = :batch_id
                UNION
                SELECT DISTINCT shipment_id FROM silver.cod_settlements WHERE batch_id = :batch_id
                UNION
                SELECT DISTINCT shipment_id FROM silver.customer_tickets WHERE batch_id = :batch_id AND shipment_id IS NOT NULL
            ),
            event_milestones AS (
                SELECT
                    e.shipment_id,
                    MIN(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN e.event_time END) AS first_sorting_time,
                    MAX(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN e.event_time END) AS last_sorting_time,
                    -- [AUDIT-D10] Lần phát = DELIVERY_ATTEMPT (thất bại) + DELIVERED; RETURNED là xử lý hoàn, không phải lần phát
                    MIN(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN e.event_time END) AS first_attempt_time,
                    MAX(CASE WHEN e.event_type = 'RETURNED' THEN e.event_time END) AS return_time,
                    COUNT(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN 1 END) AS delivery_attempt_count
                FROM silver.shipment_tracking_events e
                JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
                GROUP BY e.shipment_id
            ),
            latest_event_reason AS (
                -- Mã lý do (status_reason_sk) + status_code của sự kiện mới nhất của vận đơn
                SELECT DISTINCT ON (e.shipment_id)
                    e.shipment_id,
                    dsr.status_reason_sk,
                    dsr.status_code
                FROM silver.shipment_tracking_events e
                JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
                LEFT JOIN silver.status_reasons ssr ON e.status_reason_id = ssr.status_reason_id
                LEFT JOIN gold.dim_status_reason dsr ON ssr.reason_code = dsr.reason_code
                ORDER BY e.shipment_id, e.event_time DESC, e.event_id DESC
            ),
            fallback_status_lookup AS (
                SELECT DISTINCT ON (status_code)
                    status_code, status_reason_sk
                FROM gold.dim_status_reason
                WHERE status_reason_sk <> -1
                ORDER BY status_code, status_reason_sk
            ),
            cod_agg AS (
                SELECT DISTINCT ON (c.shipment_id)
                    c.shipment_id,
                    c.settlement_status,
                    c.settled_at
                FROM silver.cod_settlements c
                JOIN impacted_shipments imp ON c.shipment_id = imp.shipment_id
                ORDER BY c.shipment_id, c.updated_at DESC
            ),
            ticket_agg AS (
                SELECT
                    t.shipment_id,
                    COUNT(t.ticket_id) AS ticket_count
                FROM silver.customer_tickets t
                JOIN impacted_shipments imp ON t.shipment_id = imp.shipment_id
                GROUP BY t.shipment_id
            )
            INSERT INTO gold.fact_shipment_lifecycle (
                shipment_id_nk, shipment_number,
                sender_customer_sk, receiver_customer_sk,
                origin_hub_sk, destination_hub_sk,
                service_type_sk, payment_method_sk, current_status_sk,
                order_date_key, pickup_date_key, first_sorting_date_key, last_sorting_date_key,
                first_delivery_attempt_date_key, final_delivery_date_key, return_date_key,
                promised_delivery_date_key, cod_settled_date_key,
                order_timestamp, pickup_timestamp, first_sorting_timestamp, last_sorting_timestamp,
                first_delivery_attempt_timestamp, final_delivery_timestamp,
                promised_delivery_timestamp, cod_settled_timestamp,
                weight, freight_charge, cod_amount, distance_km,
                delivery_attempt_count, ticket_count,
                pickup_to_sorting_hours, sorting_to_delivery_hours, total_lead_time_hours,
                is_delivered, is_returned, is_cancelled, is_on_time, is_sla_breached,
                is_cod_settled, has_complaint_ticket,
                created_at, updated_at, batch_id
            )
            SELECT
                s.shipment_id,
                s.shipment_number,
                COALESCE(dc_send.customer_sk, -1) AS sender_customer_sk,
                COALESCE(dc_recv.customer_sk, -1) AS receiver_customer_sk,
                COALESCE(dh_orig.hub_sk, -1) AS origin_hub_sk,
                COALESCE(dh_dest.hub_sk, -1) AS destination_hub_sk,
                COALESCE(dst.service_type_sk, -1) AS service_type_sk,
                COALESCE(dpm.payment_method_sk, -1) AS payment_method_sk,
                COALESCE(
                    CASE WHEN ler.status_code = s.current_status THEN ler.status_reason_sk END,
                    fsl_st.status_reason_sk,
                    -1
                ) AS current_status_sk,

                gold.fn_date_key(s.order_date) AS order_date_key,
                gold.fn_date_key(s.pickup_date) AS pickup_date_key,
                gold.fn_date_key(m.first_sorting_time) AS first_sorting_date_key,
                gold.fn_date_key(m.last_sorting_time) AS last_sorting_date_key,
                gold.fn_date_key(m.first_attempt_time) AS first_delivery_attempt_date_key,
                CASE WHEN s.current_status = 'DELIVERED' THEN gold.fn_date_key(s.actual_delivery_date) ELSE -1 END AS final_delivery_date_key,
                CASE WHEN s.current_status = 'RETURNED' THEN gold.fn_date_key(COALESCE(m.return_time, s.actual_delivery_date)) ELSE -1 END AS return_date_key,
                gold.fn_date_key(s.promised_delivery_date) AS promised_delivery_date_key,
                CASE WHEN ca.settlement_status = 'SETTLED' THEN gold.fn_date_key(ca.settled_at) ELSE -1 END AS cod_settled_date_key,

                s.order_date AS order_timestamp,
                s.pickup_date AS pickup_timestamp,
                m.first_sorting_time AS first_sorting_timestamp,
                m.last_sorting_time AS last_sorting_timestamp,
                m.first_attempt_time AS first_delivery_attempt_timestamp,
                CASE WHEN s.current_status = 'DELIVERED' THEN s.actual_delivery_date END AS final_delivery_timestamp,
                s.promised_delivery_date AS promised_delivery_timestamp,
                CASE WHEN ca.settlement_status = 'SETTLED' THEN ca.settled_at END AS cod_settled_timestamp,

                s.weight,
                s.freight_charge,
                s.cod_amount,
                s.distance_km,
                COALESCE(m.delivery_attempt_count, 0) AS delivery_attempt_count,
                COALESCE(ta.ticket_count, 0) AS ticket_count,

                ROUND((EXTRACT(EPOCH FROM (m.first_sorting_time - s.pickup_date)) / 3600.0)::NUMERIC, 2) AS pickup_to_sorting_hours,
                CASE WHEN s.current_status = 'DELIVERED'
                     THEN ROUND((EXTRACT(EPOCH FROM (s.actual_delivery_date - m.first_sorting_time)) / 3600.0)::NUMERIC, 2)
                END AS sorting_to_delivery_hours,
                CASE WHEN s.current_status = 'DELIVERED'
                     THEN ROUND((EXTRACT(EPOCH FROM (s.actual_delivery_date - s.order_date)) / 3600.0)::NUMERIC, 2)
                END AS total_lead_time_hours,

                (s.current_status = 'DELIVERED') AS is_delivered,
                (s.current_status = 'RETURNED') AS is_returned,
                (s.current_status = 'CANCELLED') AS is_cancelled,
                CASE
                    WHEN s.current_status = 'DELIVERED' AND s.actual_delivery_date <= s.promised_delivery_date THEN TRUE
                    ELSE FALSE
                END AS is_on_time,
                -- SLA breach = đơn ĐÃ GIAO nhưng trễ hẹn -> is_on_time + is_sla_breached = is_delivered
                CASE
                    WHEN s.current_status = 'DELIVERED' AND s.actual_delivery_date > s.promised_delivery_date THEN TRUE
                    ELSE FALSE
                END AS is_sla_breached,
                (COALESCE(ca.settlement_status, 'NONE') = 'SETTLED') AS is_cod_settled,
                (COALESCE(ta.ticket_count, 0) > 0) AS has_complaint_ticket,

                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                :batch_id
            FROM silver.shipments s
            JOIN impacted_shipments imp ON s.shipment_id = imp.shipment_id
            LEFT JOIN event_milestones m ON s.shipment_id = m.shipment_id
            LEFT JOIN latest_event_reason ler ON s.shipment_id = ler.shipment_id
            LEFT JOIN fallback_status_lookup fsl_st ON s.current_status = fsl_st.status_code
            LEFT JOIN cod_agg ca ON s.shipment_id = ca.shipment_id
            LEFT JOIN ticket_agg ta ON s.shipment_id = ta.shipment_id
            LEFT JOIN silver.customers sc_send ON s.sender_customer_id = sc_send.customer_id
            -- Point-in-Time SCD2 Join cho Sender Customer
            LEFT JOIN LATERAL (
                SELECT customer_sk FROM gold.dim_customer c
                WHERE c.customer_nk = sc_send.customer_code
                  AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dc_send ON TRUE
            LEFT JOIN silver.customers sc_recv ON s.receiver_customer_id = sc_recv.customer_id
            -- Point-in-Time SCD2 Join cho Receiver Customer
            LEFT JOIN LATERAL (
                SELECT customer_sk FROM gold.dim_customer c
                WHERE c.customer_nk = sc_recv.customer_code
                  AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dc_recv ON TRUE
            LEFT JOIN silver.hubs sh_orig ON s.origin_hub_id = sh_orig.hub_id
            LEFT JOIN gold.dim_hub dh_orig ON sh_orig.hub_code = dh_orig.hub_nk
            LEFT JOIN silver.hubs sh_dest ON s.destination_hub_id = sh_dest.hub_id
            LEFT JOIN gold.dim_hub dh_dest ON sh_dest.hub_code = dh_dest.hub_nk
            LEFT JOIN silver.service_types sst ON s.service_type_id = sst.service_type_id
            LEFT JOIN gold.dim_service_type dst ON sst.service_code = dst.service_type_nk
            LEFT JOIN silver.payment_methods spm ON s.payment_method_id = spm.payment_method_id
            LEFT JOIN gold.dim_payment_method dpm ON spm.payment_method_code = dpm.payment_method_nk
            -- [AUDIT-D9] Upsert theo khóa nguồn bất biến shipment_id (số vận đơn có thể bị sửa ở nguồn)
            ON CONFLICT (shipment_id_nk) DO UPDATE SET
                shipment_number = EXCLUDED.shipment_number,
                sender_customer_sk = EXCLUDED.sender_customer_sk,
                receiver_customer_sk = EXCLUDED.receiver_customer_sk,
                origin_hub_sk = EXCLUDED.origin_hub_sk,
                destination_hub_sk = EXCLUDED.destination_hub_sk,
                service_type_sk = EXCLUDED.service_type_sk,
                payment_method_sk = EXCLUDED.payment_method_sk,
                current_status_sk = EXCLUDED.current_status_sk,
                order_date_key = EXCLUDED.order_date_key,
                pickup_date_key = EXCLUDED.pickup_date_key,
                first_sorting_date_key = EXCLUDED.first_sorting_date_key,
                last_sorting_date_key = EXCLUDED.last_sorting_date_key,
                first_delivery_attempt_date_key = EXCLUDED.first_delivery_attempt_date_key,
                final_delivery_date_key = EXCLUDED.final_delivery_date_key,
                return_date_key = EXCLUDED.return_date_key,
                promised_delivery_date_key = EXCLUDED.promised_delivery_date_key,
                cod_settled_date_key = EXCLUDED.cod_settled_date_key,
                order_timestamp = EXCLUDED.order_timestamp,
                pickup_timestamp = EXCLUDED.pickup_timestamp,
                first_sorting_timestamp = EXCLUDED.first_sorting_timestamp,
                last_sorting_timestamp = EXCLUDED.last_sorting_timestamp,
                first_delivery_attempt_timestamp = EXCLUDED.first_delivery_attempt_timestamp,
                final_delivery_timestamp = EXCLUDED.final_delivery_timestamp,
                promised_delivery_timestamp = EXCLUDED.promised_delivery_timestamp,
                cod_settled_timestamp = EXCLUDED.cod_settled_timestamp,
                weight = EXCLUDED.weight,
                freight_charge = EXCLUDED.freight_charge,
                cod_amount = EXCLUDED.cod_amount,
                distance_km = EXCLUDED.distance_km,
                delivery_attempt_count = EXCLUDED.delivery_attempt_count,
                ticket_count = EXCLUDED.ticket_count,
                pickup_to_sorting_hours = EXCLUDED.pickup_to_sorting_hours,
                sorting_to_delivery_hours = EXCLUDED.sorting_to_delivery_hours,
                total_lead_time_hours = EXCLUDED.total_lead_time_hours,
                is_delivered = EXCLUDED.is_delivered,
                is_returned = EXCLUDED.is_returned,
                is_cancelled = EXCLUDED.is_cancelled,
                is_on_time = EXCLUDED.is_on_time,
                is_sla_breached = EXCLUDED.is_sla_breached,
                is_cod_settled = EXCLUDED.is_cod_settled,
                has_complaint_ticket = EXCLUDED.has_complaint_ticket,
                updated_at = CURRENT_TIMESTAMP,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 2. NẠP FACT_TRACKING_EVENTS (TRANSACTION FACT - UPSERT THEO EVENT_ID_NK)
        #   - [AUDIT-D5] Nạp lại TOÀN BỘ sự kiện của các vận đơn bị ảnh hưởng (vận đơn đổi trong batch HOẶC có sự kiện mới):
        #       + Sự kiện đến muộn chen giữa hành trình -> event_sequence của các sự kiện sau được đánh lại đúng thứ tự
        #       + Vận đơn cha đến muộn / vừa được sửa hết Blocker -> các sự kiện đang chờ ở Silver được đẩy lên Gold
        #   - Chỉ UPDATE khi giá trị thực sự đổi (IS DISTINCT FROM) để batch_id phản ánh đúng dòng thay đổi
        #     (Fact_Daily_Hub_Snapshot dựa vào batch_id này để biết ngày nào cần tính lại)
        #   - Point-in-Time Join Dim_Courier (SCD2) theo event_time
        # =====================================================================
        conn.execute(text("""
            WITH impacted_shipments AS (
                SELECT shipment_id FROM silver.shipments WHERE batch_id = :batch_id
                UNION
                SELECT shipment_id FROM silver.shipment_tracking_events WHERE batch_id = :batch_id
            ),
            seq_events AS (
                SELECT
                    e.*,
                    ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time, e.event_id) AS event_seq
                FROM silver.shipment_tracking_events e
                JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
            )
            INSERT INTO gold.fact_tracking_events AS tgt (
                event_id_nk, shipment_sk, shipment_number, event_date_key, event_timestamp,
                event_type, hub_sk, courier_sk, vehicle_sk, status_reason_sk,
                event_sequence, scan_source, batch_id, loaded_at
            )
            SELECT
                e.event_id AS event_id_nk,
                COALESCE(fsl.shipment_sk, -1) AS shipment_sk,
                ss.shipment_number,
                gold.fn_date_key(e.event_time) AS event_date_key,
                e.event_time AS event_timestamp,
                e.event_type,
                COALESCE(dh.hub_sk, -1) AS hub_sk,
                COALESCE(dco.courier_sk, -1) AS courier_sk,
                COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
                COALESCE(dsr.status_reason_sk, -1) AS status_reason_sk,
                e.event_seq AS event_sequence,
                e.scan_source,
                :batch_id,
                CURRENT_TIMESTAMP
            FROM seq_events e
            JOIN silver.shipments ss ON e.shipment_id = ss.shipment_id
            LEFT JOIN gold.fact_shipment_lifecycle fsl ON ss.shipment_id = fsl.shipment_id_nk
            LEFT JOIN silver.hubs sh ON e.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.couriers sco ON e.courier_id = sco.courier_id
            -- Join SCD2 Dim_Courier theo đúng thời điểm phát sinh sự kiện (event_time)
            LEFT JOIN LATERAL (
                SELECT courier_sk FROM gold.dim_courier c
                WHERE c.courier_nk = sco.courier_code
                  AND e.event_time >= c.valid_from AND e.event_time < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dco ON TRUE
            LEFT JOIN silver.vehicles sv ON e.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk
            LEFT JOIN silver.status_reasons ssr ON e.status_reason_id = ssr.status_reason_id
            LEFT JOIN gold.dim_status_reason dsr ON ssr.reason_code = dsr.reason_code
            ON CONFLICT (event_id_nk) DO UPDATE SET
                shipment_sk = EXCLUDED.shipment_sk,
                shipment_number = EXCLUDED.shipment_number,
                event_date_key = EXCLUDED.event_date_key,
                event_timestamp = EXCLUDED.event_timestamp,
                event_type = EXCLUDED.event_type,
                hub_sk = EXCLUDED.hub_sk,
                courier_sk = EXCLUDED.courier_sk,
                vehicle_sk = EXCLUDED.vehicle_sk,
                status_reason_sk = EXCLUDED.status_reason_sk,
                event_sequence = EXCLUDED.event_sequence,
                scan_source = EXCLUDED.scan_source,
                batch_id = EXCLUDED.batch_id,
                loaded_at = CURRENT_TIMESTAMP
            WHERE (tgt.shipment_sk, tgt.shipment_number, tgt.event_date_key, tgt.event_timestamp, tgt.event_type,
                   tgt.hub_sk, tgt.courier_sk, tgt.vehicle_sk, tgt.status_reason_sk, tgt.event_sequence, tgt.scan_source)
                  IS DISTINCT FROM
                  (EXCLUDED.shipment_sk, EXCLUDED.shipment_number, EXCLUDED.event_date_key, EXCLUDED.event_timestamp, EXCLUDED.event_type,
                   EXCLUDED.hub_sk, EXCLUDED.courier_sk, EXCLUDED.vehicle_sk, EXCLUDED.status_reason_sk, EXCLUDED.event_sequence, EXCLUDED.scan_source);
        """), {"batch_id": batch_id})

        # =====================================================================
        # 3. NẠP FACT_DAILY_HUB_SNAPSHOT (PERIODIC SNAPSHOT - IDEMPOTENT UPSERT)
        #   - [AUDIT-D6] Lưới ngày LIÊN TỤC (dense) từ ngày sớm nhất có sự kiện thay đổi trong batch tới ngày hoạt động cuối:
        #       + Sự kiện đến muộn ở ngày D làm thay đổi tồn kho của MỌI ngày sau D, không chỉ ngày D
        #       + Ngày không có lượt quét nào vẫn phải có dòng snapshot (hàng vẫn nằm trong kho)
        #   - [AUDIT-D7] pending_delivery_count = số đơn đang tồn tại CHÍNH hub đích (chờ giao chặng cuối) cuối ngày
        #     (trước đây đếm sự kiện DELIVERY_ATTEMPT - không phải "đơn chờ giao" và luôn = 0 với dữ liệu hiện tại)
        #   - CROSS JOIN (date_hub_grid) để mọi Hub đều có mặt trong ngày Snapshot
        #   - Kiểm tra trạng thái COD theo đúng thời điểm lịch sử cuối ngày snapshot
        # =====================================================================
        conn.execute(text("""
            WITH affected_window AS (
                SELECT
                    MIN(event_date_key) AS start_key,
                    (SELECT MAX(event_date_key) FROM gold.fact_tracking_events) AS end_key
                FROM gold.fact_tracking_events
                WHERE batch_id = :batch_id AND event_date_key <> -1
            ),
            snapshot_dates AS (
                SELECT d.date_key AS snapshot_date_key, d.full_date AS snapshot_date
                FROM affected_window w
                JOIN gold.dim_date d ON d.date_key BETWEEN w.start_key AND w.end_key
            ),
            date_hub_grid AS (
                SELECT sd.snapshot_date_key, sd.snapshot_date, h.hub_sk
                FROM snapshot_dates sd
                CROSS JOIN gold.dim_hub h
                WHERE h.hub_sk <> -1 AND h.is_current = TRUE
            ),
            hub_daily_flow AS (
                SELECT
                    e.event_date_key AS snapshot_date_key,
                    e.hub_sk,
                    COUNT(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN 1 END) AS inbound_count,
                    COUNT(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN 1 END) AS outbound_count,
                    COUNT(CASE WHEN e.event_type = 'RETURNED' THEN 1 END) AS return_count
                FROM gold.fact_tracking_events e
                JOIN snapshot_dates sd ON e.event_date_key = sd.snapshot_date_key
                WHERE e.hub_sk <> -1
                GROUP BY e.event_date_key, e.hub_sk
            ),
            hub_backlog_shipments AS (
                -- Danh sách đơn hàng tồn kho duy nhất tại mỗi Hub vào cuối ngày
                SELECT DISTINCT
                    g.snapshot_date_key,
                    g.snapshot_date,
                    g.hub_sk,
                    in_ev.shipment_number,
                    fsl.destination_hub_sk,
                    fsl.cod_amount,
                    fsl.cod_settled_timestamp
                FROM date_hub_grid g
                JOIN gold.fact_tracking_events in_ev
                  ON in_ev.hub_sk = g.hub_sk
                 AND in_ev.event_date_key <= g.snapshot_date_key
                 AND in_ev.event_type = 'INBOUND_SCAN'
                JOIN gold.fact_shipment_lifecycle fsl ON in_ev.shipment_number = fsl.shipment_number
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.fact_tracking_events out_ev
                    WHERE out_ev.shipment_number = in_ev.shipment_number
                      AND out_ev.hub_sk = in_ev.hub_sk
                      AND out_ev.event_type IN ('OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
                      AND out_ev.event_date_key <= g.snapshot_date_key
                      AND out_ev.event_timestamp >= in_ev.event_timestamp
                )
            ),
            hub_backlog_agg AS (
                SELECT
                    b.snapshot_date_key,
                    b.hub_sk,
                    COUNT(b.shipment_number) AS backlog_count,
                    COUNT(CASE WHEN b.hub_sk = b.destination_hub_sk THEN 1 END) AS pending_delivery_count,
                    COALESCE(SUM(
                        CASE
                            WHEN b.cod_amount > 0
                             AND (b.cod_settled_timestamp IS NULL OR b.cod_settled_timestamp::DATE > b.snapshot_date)
                            THEN b.cod_amount
                            ELSE 0
                        END
                    ), 0) AS cod_held_amount
                FROM hub_backlog_shipments b
                GROUP BY b.snapshot_date_key, b.hub_sk
            )
            INSERT INTO gold.fact_daily_hub_snapshot (
                snapshot_date_key, hub_sk,
                inbound_count, outbound_count, backlog_count,
                pending_delivery_count, return_count, cod_held_amount,
                batch_id, snapshot_generated_at
            )
            SELECT
                g.snapshot_date_key,
                g.hub_sk,
                COALESCE(f.inbound_count, 0) AS inbound_count,
                COALESCE(f.outbound_count, 0) AS outbound_count,
                COALESCE(b.backlog_count, 0) AS backlog_count,
                COALESCE(b.pending_delivery_count, 0) AS pending_delivery_count,
                COALESCE(f.return_count, 0) AS return_count,
                COALESCE(b.cod_held_amount, 0) AS cod_held_amount,
                :batch_id,
                CURRENT_TIMESTAMP
            FROM date_hub_grid g
            LEFT JOIN hub_daily_flow f
              ON g.snapshot_date_key = f.snapshot_date_key AND g.hub_sk = f.hub_sk
            LEFT JOIN hub_backlog_agg b
              ON g.snapshot_date_key = b.snapshot_date_key AND g.hub_sk = b.hub_sk
            ON CONFLICT (snapshot_date_key, hub_sk) DO UPDATE SET
                inbound_count = EXCLUDED.inbound_count,
                outbound_count = EXCLUDED.outbound_count,
                backlog_count = EXCLUDED.backlog_count,
                pending_delivery_count = EXCLUDED.pending_delivery_count,
                return_count = EXCLUDED.return_count,
                cod_held_amount = EXCLUDED.cod_held_amount,
                batch_id = EXCLUDED.batch_id,
                snapshot_generated_at = CURRENT_TIMESTAMP;
        """), {"batch_id": batch_id})

        conn.commit()
        print("✅ Đã nạp hoàn tất 3 bảng Facts!")
```

### File: `scripts/refresh_views.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn
from scripts.extract_incremental import TABLES_TO_EXTRACT

def run_post_load_and_refresh_views(batch_id: str):
    print(f"🔄 [Batch: {batch_id}] Refresh Materialized Views & Cập nhật Watermark / Audit Log...")

    with get_dwh_conn() as conn:
        # 1. Refresh các Materialized Views trong tầng Gold
        conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_daily_hub_performance;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_courier_performance;"))

        # 2. Cập nhật Watermark và hoàn tất Batch Log cho từng bảng
        # [AUDIT-A3] Watermark mới = thời điểm snapshot OLTP của batch (source_snapshot_ts), KHÔNG phải MAX(updated_at):
        #   Trước đây lấy MAX(updated_at); chỉ cần 1 dòng nguồn có updated_at ở TƯƠNG LAI (lệch đồng hồ, dữ liệu giả lập
        #   "giao trong tương lai"...) là watermark nhảy vượt hiện tại -> mọi thay đổi thật sau đó bị bỏ qua (đo được:
        #   lần chạy 2 extract 0 shipment / 0 COD). Dòng có updated_at > snapshot vẫn được đọc lại ở batch sau (idempotent).
        #   Chỉ cập nhật ở bước cuối -> batch lỗi giữa chừng thì lần sau đọc lại từ watermark cũ.
        for table in TABLES_TO_EXTRACT:
            conn.execute(text("""
                UPDATE meta.etl_watermark w
                SET last_extracted_timestamp = COALESCE(b.source_snapshot_ts, w.last_extracted_timestamp),
                    last_batch_id = :batch_id,
                    updated_at = CURRENT_TIMESTAMP
                FROM meta.etl_batch_log b
                WHERE w.table_name = :table_name
                  AND b.batch_id = :batch_id
                  AND b.table_name = w.table_name;
            """), {"batch_id": batch_id, "table_name": table})

            conn.execute(text("""
                UPDATE meta.etl_batch_log
                SET run_end = CURRENT_TIMESTAMP,
                    rows_loaded = GREATEST(rows_extracted - rows_rejected, 0),
                    status = 'SUCCESS'
                WHERE batch_id = :batch_id AND table_name = :table_name;
            """), {"batch_id": batch_id, "table_name": table})

        conn.commit()
        print("✅ Đã refresh Materialized Views và cập nhật Watermark thành công!")


def mark_batch_failed(batch_id: str, error_message: str):
    """[AUDIT-G1] Đánh dấu FAILED cho mọi bảng chưa SUCCESS của batch khi một bước sau extract bị lỗi.
    Trước đây batch lỗi ở DQ/Dim/Fact bị kẹt ở trạng thái EXTRACTED, không ai biết batch đã hỏng."""
    with get_dwh_conn() as conn:
        conn.execute(text("""
            UPDATE meta.etl_batch_log
            SET status = 'FAILED', run_end = CURRENT_TIMESTAMP, error_message = LEFT(:err, 4000)
            WHERE batch_id = :batch_id AND status <> 'SUCCESS';
        """), {"batch_id": batch_id, "err": error_message})
        conn.commit()
```

### File: `scripts/run_local_pipeline.py`

```python
import sys
import uuid
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
# [AUDIT-G2] Cho phép chạy trực tiếp "python scripts/run_local_pipeline.py" từ thư mục gốc (Windows/Local)
# mà không cần set PYTHONPATH; trong Docker PYTHONPATH=/opt/airflow vẫn hoạt động như cũ.
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import text
from config.db_config import get_oltp_conn, get_dwh_conn
from scripts.generate_oltp_data import generate_data
from scripts.extract_incremental import run_extract_incremental
from scripts.data_quality_checks import run_data_quality_and_silver_load
from scripts.load_dimensions import run_load_dimensions
from scripts.load_facts import run_load_facts
from scripts.refresh_views import run_post_load_and_refresh_views, mark_batch_failed


def execute_sql_file(conn, file_path: Path):
    sql_content = file_path.read_text(encoding="utf-8")
    conn.execute(text(sql_content))
    conn.commit()


def init_databases():
    print("🛠️ Đang khởi tạo DDL cho OLTP và DWH...")
    with get_oltp_conn() as oltp_conn:
        execute_sql_file(oltp_conn, BASE_DIR / "sql/oltp/01_create_oltp_tables.sql")
        execute_sql_file(oltp_conn, BASE_DIR / "sql/oltp/02_seed_constraints.sql")

    with get_dwh_conn() as dwh_conn:
        for sql_file in sorted((BASE_DIR / "sql/dwh").glob("*.sql")):
            execute_sql_file(dwh_conn, sql_file)
        for sql_file in sorted((BASE_DIR / "sql/analytics").glob("*.sql")):
            execute_sql_file(dwh_conn, sql_file)


def run_pipeline():
    batch_id = f"BATCH_{uuid.uuid4().hex[:10].upper()}"
    print("=" * 75)
    print(f"🚀 KHỞI CHẠY EXPRESS DELIVERY DWH PIPELINE | BATCH_ID: {batch_id}")
    print("=" * 75)

    init_databases()
    generate_data()

    # [AUDIT-G1] Lỗi ở bất kỳ bước nào -> đánh dấu batch FAILED trong meta.etl_batch_log rồi mới ném lỗi ra ngoài
    try:
        run_extract_incremental(batch_id)
        run_data_quality_and_silver_load(batch_id)
        run_load_dimensions(batch_id)
        run_load_facts(batch_id)
        run_post_load_and_refresh_views(batch_id)
    except Exception as exc:
        mark_batch_failed(batch_id, repr(exc))
        print(f"❌ Batch {batch_id} THẤT BẠI: {exc}")
        raise

    # In báo cáo tổng kết
    with get_dwh_conn() as conn:
        fsl_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_shipment_lifecycle")).scalar()
        fte_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_tracking_events")).scalar()
        fdhs_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_daily_hub_snapshot")).scalar()
        quar_cnt = conn.execute(text("SELECT COUNT(*) FROM meta.data_quarantine")).scalar()
        print("=" * 75)
        print(f"🎉 PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print(f"   • Fact_Shipment_Lifecycle : {fsl_cnt:,} dòng")
        print(f"   • Fact_Tracking_Events    : {fte_cnt:,} dòng")
        print(f"   • Fact_Daily_Hub_Snapshot : {fdhs_cnt:,} dòng")
        print(f"   • Meta.Data_Quarantine    : {quar_cnt:,} bản ghi lỗi/cảnh báo đã bắt được")
        print("=" * 75)


if __name__ == "__main__":
    run_pipeline()
```

---

## 7. Đóng gói Orchestration bằng Apache Airflow (`dags/`)

### File: `dags/express_delivery_dwh_dag.py`

```python
import sys
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import get_oltp_conn, get_dwh_conn
from scripts.extract_incremental import run_extract_incremental
from scripts.data_quality_checks import run_data_quality_and_silver_load
from scripts.load_dimensions import run_load_dimensions
from scripts.load_facts import run_load_facts
from scripts.refresh_views import run_post_load_and_refresh_views, mark_batch_failed


def _batch_id(context) -> str:
    # Mọi task (và mọi lần retry) của cùng một DAG Run dùng chung 1 batch_id -> các bước idempotent theo batch.
    # [AUDIT-G3] run_id do API/CLI tự đặt có thể dài hơn 64 ký tự của cột batch_id -> rút gọn bằng hash.
    run_id = context["run_id"]
    batch_id = f"AF_{run_id}"
    return batch_id if len(batch_id) <= 64 else "AF_" + hashlib.md5(run_id.encode("utf-8")).hexdigest()


def _on_task_failure(context):
    # [AUDIT-G1] Task hỏng sau khi hết retry -> đánh dấu batch FAILED trong meta.etl_batch_log
    mark_batch_failed(_batch_id(context), str(context.get("exception")))


def task_preflight_checks(**context):
    with get_oltp_conn() as oltp_conn:
        oltp_conn.execute(text("SELECT 1 FROM shipments LIMIT 1"))
    with get_dwh_conn() as dwh_conn:
        dwh_conn.execute(text("SELECT 1 FROM meta.etl_watermark LIMIT 1"))
    print("✅ Preflight checks: Kết nối OLTP, DWH và các bảng Meta đều sẵn sàng.")

def task_extract_incremental(**context):
    run_extract_incremental(_batch_id(context))

def task_data_quality_gate(**context):
    run_data_quality_and_silver_load(_batch_id(context))

def task_load_dimensions(**context):
    run_load_dimensions(_batch_id(context))

def task_load_facts(**context):
    run_load_facts(_batch_id(context))

def task_refresh_views_and_watermark(**context):
    run_post_load_and_refresh_views(_batch_id(context))

default_args = {
    "owner": "express_dwh_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _on_task_failure,
}

with DAG(
    dag_id="express_delivery_dwh_dag",
    default_args=default_args,
    description="Enterprise Medallion + Kimball ETL Pipeline cho Express Delivery DWH",
    schedule="0 */4 * * *",   # schedule_interval đã deprecated từ Airflow 2.4
    catchup=False,
    # [AUDIT-G3] Không cho 2 DAG Run chạy song song: các batch cùng ghi Silver/Gold và cùng đẩy watermark
    max_active_runs=1,
    tags=["express_delivery", "medallion", "kimball", "scd2"]
) as dag:

    preflight_checks = PythonOperator(
        task_id="preflight_checks",
        python_callable=task_preflight_checks
    )

    extract_incremental = PythonOperator(
        task_id="extract_incremental",
        python_callable=task_extract_incremental
    )

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=task_data_quality_gate
    )

    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions
    )

    load_facts = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts
    )

    refresh_views = PythonOperator(
        task_id="refresh_views_and_update_watermark",
        python_callable=task_refresh_views_and_watermark
    )

    preflight_checks >> extract_incremental >> data_quality_gate >> load_dimensions >> load_facts >> refresh_views
```

---

## 8. Đóng gói Docker Compose (`docker/`)

### File: `docker/requirements.txt`

```text
SQLAlchemy>=1.4.49,<2.0
psycopg2-binary==2.9.9
pandas==2.1.4
Faker==25.2.0
```

### File: `docker/docker-compose.yml`

```yaml
# [AUDIT-G4] Bỏ khóa "version" (Compose v2 đã bỏ, chỉ sinh cảnh báo)
services:
  postgres-db:
    image: postgres:15
    container_name: express_postgres
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=airflow_meta
    ports:
      - "5432:5432"
    volumes:
      - pg_dwh_data:/var/lib/postgresql/data
      # [AUDIT-G4] Tạo express_oltp / express_dwh bằng cơ chế init chuẩn của image postgres (chạy 1 lần khi volume mới).
      # Trước đây dùng "sleep 5" rồi psql: lần khởi tạo đầu (initdb) thường lâu hơn 5s -> DB không được tạo.
      - ./init-db.sql:/docker-entrypoint-initdb.d/01_init_db.sql:ro
    healthcheck:
      # Chỉ "healthy" khi DB express_dwh đã thật sự tồn tại (pg_isready báo sẵn sàng cả khi DB chưa được tạo)
      test: ["CMD-SHELL", "psql -U postgres -d express_dwh -c 'SELECT 1' > /dev/null 2>&1"]
      interval: 5s
      timeout: 5s
      retries: 20

  airflow:
    image: apache/airflow:2.9.1-python3.11
    container_name: express_airflow
    depends_on:
      postgres-db:
        condition: service_healthy
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres-db:5432/airflow_meta
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - OLTP_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-db:5432/express_oltp
      - DWH_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-db:5432/express_dwh
      - PYTHONPATH=/opt/airflow
    volumes:
      - ../dags:/opt/airflow/dags
      - ../scripts:/opt/airflow/scripts
      - ../config:/opt/airflow/config
      - ../sql:/opt/airflow/sql
      - ./requirements.txt:/opt/airflow/requirements.txt
    ports:
      - "8080:8080"
    # [AUDIT-G4] Pipeline lần đầu lỗi vẫn khởi động Airflow UI để xem log / chạy lại; scheduler chạy foreground (exec)
    command: >
      bash -c "
      pip install --no-cache-dir -r /opt/airflow/requirements.txt &&
      export PYTHONPATH=/opt/airflow &&
      airflow db migrate &&
      (airflow users create --username admin --password admin --firstname Express --lastname Admin --role Admin --email admin@express.vn || true) &&
      (python /opt/airflow/scripts/run_local_pipeline.py || echo '⚠️ Pipeline khởi tạo lỗi - xem log phía trên và bảng meta.etl_batch_log') ;
      airflow webserver &
      exec airflow scheduler
      "

volumes:
  pg_dwh_data:
```

### File: `docker/init-db.sql`

```sql
-- [AUDIT-G4] Chạy tự động 1 lần khi volume Postgres còn trống (docker-entrypoint-initdb.d)
CREATE DATABASE express_oltp;
CREATE DATABASE express_dwh;
```

---

## 9. Hướng dẫn chạy thực tế (Local & Docker) và Kiểm thử khi bảo vệ

### Cách 1: Chạy trực tiếp bằng Docker Compose (Tự động 100%)

```bash
cd docker
docker-compose down -v   # Bắt buộc nếu từng chạy bản cũ: xóa volume để init-db.sql chạy lại và DDL mới được áp dụng
docker-compose up -d
```

* Container `express_postgres` tự tạo 2 database `express_oltp` và `express_dwh` qua `init-db.sql` (chạy 1 lần khi volume mới).
* Container `express_airflow` cài đúng gói trong `requirements.txt`, khởi chạy `run_local_pipeline.py` để tạo toàn bộ bảng, sinh **10,000 Shipments + ~58,000 Tracking Events + ~3,900 COD Settlements + ~500 Customer Tickets**, chạy ETL lần 1 rồi mở Airflow UI tại `http://localhost:8080` (`admin` / `admin`). Nếu pipeline lần đầu lỗi, Airflow UI vẫn khởi động; xem log container và bảng `meta.etl_batch_log`.
* Để demo **Incremental Load + SCD Type 1/2 + Upsert Accumulating Fact**, chạy lệnh dưới. Lần chạy thứ 2 trở đi sẽ mô phỏng: 25 khách đổi hạng (SCD2), 15 shipper điều chuyển Hub có ngày hiệu lực (SCD2), 1 Hub đổi tên (SCD1), 50 đơn đang đi được giao thành công (kèm sự kiện quét), 40 khoản COD được quyết toán và 200 vận đơn mới:

```bash
docker exec -it express_airflow python /opt/airflow/scripts/run_local_pipeline.py
```

* Không cần tạm dừng DAG khi chạy tay: nếu một batch khác đang chạy dở, batch mới sẽ dừng ngay với thông báo rõ ràng và không ghi dữ liệu gì.

### Cách 1b: Chạy Local trên Windows (không Docker)

```powershell
# Yêu cầu: PostgreSQL đã có 2 database express_oltp, express_dwh; đã pip install -r docker/requirements.txt
$env:OLTP_DB_URI = "postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
$env:DWH_DB_URI  = "postgresql+psycopg2://postgres:postgres@localhost:5432/express_dwh"
python scripts\run_local_pipeline.py   # chạy từ thư mục gốc dự án; lần 1 = full load, các lần sau = incremental
```

Nếu database đã tạo bằng bản cũ, dựng lại trước khi chạy: `DROP SCHEMA stg, silver, gold, meta CASCADE;` trên `express_dwh` và xóa toàn bộ bảng trên `express_oltp` (hoặc tạo lại 2 database).

### Cách 2: Truy vấn SQL kiểm chứng để trình chiếu khi bảo vệ đồ án

```sql
-- 1. Kiểm tra Audit Log và Watermark sau các lần chạy
SELECT * FROM meta.etl_watermark;
SELECT batch_id, table_name, rows_extracted, rows_loaded, rows_rejected, status FROM meta.etl_batch_log ORDER BY run_start DESC;

-- 2. Kiểm tra các bản ghi bẩn bị chặn tại cổng Data Quality Gate
SELECT source_table, rule_name, severity, COUNT(*) AS total_violations
FROM meta.data_quarantine
GROUP BY source_table, rule_name, severity;

-- 3. Chứng minh SCD Type 2 lưu giữ lịch sử đổi hạng của Khách hàng và chuyển Hub của Shipper (Denormalized)
SELECT courier_nk, courier_name, assigned_hub_code, assigned_hub_name, contract_type, valid_from, valid_to, is_current
FROM gold.dim_courier
WHERE courier_nk IN (SELECT courier_nk FROM gold.dim_courier GROUP BY courier_nk HAVING COUNT(*) > 1)
ORDER BY courier_nk, valid_from;

-- 4. Kiểm tra Phân tích nguyên nhân hoàn hàng (Đã phân biệt rõ RS_REFUSED và RS_WRONG_ADDR)
SELECT sr.status_code, sr.reason_code, sr.reason_name, COUNT(*) AS shipment_count
FROM gold.fact_shipment_lifecycle f
JOIN gold.dim_status_reason sr ON f.current_status_sk = sr.status_reason_sk
WHERE f.is_returned = TRUE
GROUP BY sr.status_code, sr.reason_code, sr.reason_name;
```


---

## 10. Nhật ký Audit & sửa lỗi toàn bộ dự án (Senior DWH Review)

**Phạm vi:** đợt 1 audit luồng dữ liệu `stg → silver → gold` (extract, watermark, DQ gate, Silver, Dimensions, Facts). Đợt 2 sửa nốt các rủi ro còn lại của đợt 1 và mở rộng sang generator OLTP, view, `run_local_pipeline.py`, DAG Airflow và Docker. Mỗi chỗ sửa trong code có tag `[AUDIT-xx]` khớp với các bảng dưới.

**Cách kiểm chứng:** tách toàn bộ file code trong tài liệu này ra và chạy thật trên PostgreSQL 16 + SQLAlchemy 1.4 + pandas 2.1.4. Bộ kiểm thử gồm:
* 2 lần `run_local_pipeline.py` (full load, rồi incremental + SCD).
* Các batch kịch bản: cân lại hàng, sửa đơn từng bị Blocker, sự kiện đến muộn, đơn `DELIVERY_FAILED`, đơn thiếu người nhận, điều chuyển shipper có ngày hiệu lực lùi 10 ngày, dời ngày đặt hàng sau sự kiện, đơn "giao trong tương lai".
* Chạy lại cùng `batch_id` (giả lập Airflow retry), hai batch chạy chồng nhau, và một batch lỗi giữa chừng.

Số liệu "bản gốc" đo trên tài liệu trước khi sửa. DAG được nạp thử với module Airflow giả lập để kiểm tra cấu hình và callback. `docker-compose.yml` được kiểm tra cú pháp YAML nhưng **chưa chạy thử bằng Docker**.

**Phần đã đúng, giữ nguyên:** phát hiện thay đổi SCD2 bằng `row_hash`, khoảng hiệu lực nửa mở `[valid_from, valid_to)`, point-in-time join theo `order_date` / `event_time`, dòng Unknown `-1` cho mọi chiều, upsert idempotent theo khóa tự nhiên, kiến trúc Medallion + Galaxy Schema.

### 10.1 Đợt 1: luồng dữ liệu stg → silver → gold

| Mã | Mức | Vị trí | Lỗi | Bằng chứng trên bản gốc | Cách sửa |
|---|---|---|---|---|---|
| A1 | Cao | `extract_incremental.py` | Mỗi bảng OLTP được đọc ở một thời điểm khác nhau, nên trong cùng batch bảng con có thể chứa bản ghi mà bảng cha chưa có | Rủi ro | Đọc cả 11 bảng trong một snapshot `REPEATABLE READ` |
| A2 | TB | `extract_incremental.py` | Điều kiện `updated_at > watermark` bỏ sót giao dịch commit muộn | Rủi ro | Đọc lùi 10 phút (`EXTRACT_LOOKBACK_MINUTES`), an toàn vì mọi tầng đều upsert idempotent |
| A3 | **Nghiêm trọng** | extract, `refresh_views.py`, `meta.etl_batch_log` | Watermark = `MAX(updated_at)`; chỉ 1 dòng có updated_at ở tương lai là watermark vượt hiện tại và mọi thay đổi sau đó bị bỏ qua | Watermark `shipments` nằm khoảng 1 ngày trong tương lai; lần chạy 2 extract **0/50** shipment và **0/40** COD | Watermark = thời điểm snapshot OLTP (`source_snapshot_ts`), chỉ ghi khi batch SUCCESS |
| A4 | Thấp | `extract_incremental.py` | Comment nói "giữ 3 batch" nhưng code không dọn gì; nhánh `except` không rollback nên không ghi được FAILED | Đọc code | Thêm retention thật; rollback trước khi ghi FAILED |
| B1 | TB | DQ Rule 1-3, Silver 3.6 | Loại trừ Blocker theo `shipment_number`; số vận đơn NULL thì không khớp và dòng lỗi lọt xuống Silver | Đọc code | `record_id = shipment_id` |
| B2 | Cao | DQ Rule 3 | `NULL NOT IN (...)` trả về NULL nên trạng thái NULL lọt qua; danh mục thiếu `DELIVERY_FAILED` | Đơn `DELIVERY_FAILED` hợp lệ bị cách ly | Kiểm tra NULL-safe, đủ danh mục, kiểm các cột NOT NULL |
| B3 | Cao | `04_create_silver_tables.sql` | FK phụ (người nhận, hub, dịch vụ, thanh toán) là NOT NULL ở Silver trong khi OLTP cho phép NULL | 1 đơn thiếu người nhận gây `NotNullViolation` và **sập cả batch** | Cho phép NULL + Warning, Gold map về SK `-1` |
| B4 | TB | DQ | Không có rule bảo vệ cột NOT NULL của sự kiện | Đọc code | `RULE_EVENT_REQUIRED_FIELDS` |
| B5 | Cao | Mọi upsert Silver | `ON CONFLICT` chỉ cập nhật một phần cột; bản cũ có thể ghi đè bản mới | Đơn cân lại vẫn giữ 22,63 kg / HUB_004 ở Silver và Gold, trong khi nguồn là 29,63 kg / HUB_001 | Cập nhật đủ cột + `WHERE tgt.updated_at <= EXCLUDED.updated_at` |
| B6 | Thấp | Silver customers | Bịa số `'0000000000'` / giữ chuỗi số rác | 39 số không hợp lệ nằm trong Silver | Số không hợp lệ thì gán NULL |
| B7 | **Nghiêm trọng** | Silver 3.7–3.9 | `INNER JOIN silver.shipments` làm event/COD/ticket của đơn bị Blocker biến mất không dấu vết, không bao giờ quay lại | Mất **836 event, 78 COD, 13 ticket**; đơn sửa xong lên Gold với 0 event | Giữ ở Silver + Warning `RULE_ORPHAN_SHIPMENT`; Gold tự đẩy lên khi đơn cha hợp lệ |
| B8 | Thấp | DQ | `rows_rejected` đếm số vi phạm thay vì số dòng | Đọc code | `COUNT(DISTINCT record_id)` |
| C1 | TB | `load_dimensions.py` | Thuộc tính hub/xe denormalize trong `dim_courier` không theo SCD1 của `dim_hub` / `dim_vehicle` | Đọc code | Bước 9: đồng bộ |
| D1 | **Nghiêm trọng** | Fact lifecycle | `current_status_sk` lấy từ sự kiện cuối, không đối chiếu trạng thái vận đơn | **1.592** đơn DELIVERED mang status IN_TRANSIT | Chỉ dùng lý do của sự kiện cuối khi status khớp |
| D2 | Cao | Fact lifecycle | Đơn RETURNED dùng ngày hoàn làm `final_delivery_timestamp`, lead time, SLA | 791 đơn hoàn có lead time; 388 bị tính "vi phạm SLA" | Chỉ tính cho DELIVERED |
| D3 | Cao | Fact lifecycle | `ON CONFLICT` không cập nhật SK chiều, measure, ngày đặt / ngày hẹn | Xem B5 | Cập nhật đủ cột |
| D4 | TB | Facts + `dim_date` | `TO_CHAR(...)::INT` trực tiếp + lịch cứng 2024–2028, nên ngày ngoài lịch làm vỡ FK | Rủi ro | `gold.fn_date_key()` + cận trên lịch tính động |
| D5 | Cao | Fact tracking events | Chỉ nạp event của batch: trùng `event_sequence`; event chờ ở Silver không bao giờ lên Gold | `event_sequence = 3` bị trùng | Nạp lại toàn bộ event của vận đơn bị ảnh hưởng |
| D6 | TB | Fact daily snapshot | Chỉ tính lại những ngày có event trong batch; ngày không có lượt quét thì không có dòng | Rủi ro | Lưới ngày liên tục từ ngày sớm nhất bị ảnh hưởng |
| D7 | TB | Fact daily snapshot | `pending_delivery_count` đếm event `DELIVERY_ATTEMPT` | Luôn = **0** | Đếm số đơn đang tồn tại ở hub đích cuối ngày |
| E1 | Thấp | `07_create_indexes.sql` | Thiếu index `batch_id` / `shipment_id` ở stg/silver/meta | Hiệu năng | Thêm 10 index |

### 10.2 Đợt 2: rủi ro còn lại, generator, view, runner, DAG, Docker

| Mã | Mức | Vị trí | Lỗi | Bằng chứng | Cách sửa |
|---|---|---|---|---|---|
| F1 | **Nghiêm trọng** | `generate_oltp_data.py` | Sinh ngày giao, `updated_at`, ngày quyết toán COD **ở tương lai**; đây là nguyên nhân dữ liệu của A3 | 4 shipment, 4 event, 12 COD ở tương lai | Mọi mốc ≤ thời điểm sinh; đơn có mốc kết thúc ở tương lai được giữ trạng thái IN_TRANSIT, COD giữ PENDING |
| F2 | Cao | generator | Timeline sai: đơn giao "trước" khi xuất kho thì không có sự kiện DELIVERED; ngày lấy hàng có thể sau ngày giao | **1.592** đơn DELIVERED không có sự kiện DELIVERED (nguyên nhân dữ liệu của D1) | Các mốc trung chuyển đặt theo tỉ lệ trong khoảng [pickup, kết thúc]; thêm `DELIVERY_ATTEMPT` thất bại (12% đơn giao, mọi đơn hoàn) |
| F3 | TB | `02_seed_constraints.sql` | `updated_at` chỉ đổi khi ứng dụng nhớ gán; ứng dụng quên thì CDC bỏ sót | Rủi ro | Trigger `BEFORE UPDATE` tự gán khi ứng dụng không đổi `updated_at` |
| F4 | Thấp | generator | Faker không được seed nên dữ liệu không tái lập được | Đọc code | `Faker.seed(42)` |
| F5 | Thấp | generator | Bưu tá được gán ngẫu nhiên toàn mạng, không thuộc hub của sự kiện | Đọc code | Chọn bưu tá thuộc hub phát sinh sự kiện |
| F6 | Cao | generator (lần chạy mô phỏng) | Khách Gold "đổi" thành Gold; đơn chuyển DELIVERED không kèm sự kiện; quyết toán COD cả đơn hoàn / đang đi; thông báo "& đơn mới" nhưng không sinh đơn mới | Chỉ 15/25 khách đổi hạng thật | Chọn đúng đối tượng; thêm sự kiện INBOUND đích + DELIVERED; chỉ quyết toán COD đơn đã giao; sinh 200 đơn mới; thêm kịch bản SCD1 đổi tên Hub |
| F7 | Cao | generator | Bản ghi chèn ở lần mô phỏng mang `updated_at` = thời gian nghiệp vụ (quá khứ), nên CDC theo `updated_at` không thấy | Phát hiện khi kiểm thử bản sửa: chỉ 25/200 đơn mới được extract | `created_at` / `updated_at` = thời điểm ghi bản ghi |
| A5 | Cao | `extract_incremental.py` | Không có gì ngăn 2 batch chạy chồng (Airflow + chạy tay), hai batch cùng ghi Silver/Gold và cùng đẩy watermark | Rủi ro | Advisory lock + kiểm tra batch đang RUNNING/EXTRACTED; batch bị chặn không ghi dòng nào |
| B9 | Cao | DQ | Không chặn mốc thời gian ở tương lai | Xem F1 | `RULE_NO_FUTURE_TIMESTAMP` (Blocker), so với thời điểm snapshot nguồn |
| B10 | TB | DQ | Rule 4 chỉ kiểm sự kiện mới so với vận đơn; nếu vận đơn bị dời `order_date` muộn hơn sự kiện đã có thì lọt | Đọc code | `RULE_ORDER_AFTER_EVENTS` (Blocker): Silver/Gold giữ phiên bản hợp lệ trước đó |
| C2 | TB | `dim_courier` (SCD2) | `valid_from` = `updated_at`, bỏ qua `couriers.effective_date` (ngày hiệu lực của quyết định điều chuyển) | Đọc code | Mốc = `effective_date` nếu muộn hơn `valid_from` hiện tại; đóng và mở phiên bản trong cùng 1 câu lệnh |
| D8 | Cao | `load_facts.py` | Không re-key Fact khi phiên bản SCD2 bị thu hẹp (thay đổi đến muộn / ngày hiệu lực lùi), nên Fact trỏ sai phiên bản | Kịch bản điều chuyển lùi 10 ngày: 96 sự kiện cần chuyển phiên bản | Bước 0 re-key courier / sender / receiver; kiểm tra: 0 Fact nằm ngoài khoảng hiệu lực |
| D9 | Thấp | Fact lifecycle | Khóa upsert là `shipment_number` (có thể bị sửa ở nguồn, sinh dòng trùng) | Đọc code | Thêm `shipment_id_nk` (khóa nguồn bất biến) làm khóa upsert |
| D10 | TB | Fact lifecycle | `delivery_attempt_count` tính cả sự kiện RETURNED (xử lý hoàn, không phải lần phát) | Đọc code | Chỉ đếm `DELIVERY_ATTEMPT` + `DELIVERED` |
| G1 | TB | `run_local_pipeline.py`, DAG | Batch lỗi ở DQ/Dim/Fact kẹt ở trạng thái EXTRACTED, không đánh dấu FAILED | Đọc code | `mark_batch_failed()` + `on_failure_callback` |
| G2 | Thấp | `run_local_pipeline.py` | Chạy ngoài Docker bằng `python scripts\run_local_pipeline.py` lỗi import nếu chưa set PYTHONPATH | Đọc code | Tự thêm thư mục gốc vào `sys.path` |
| G3 | Cao | DAG | Không đặt `max_active_runs` (mặc định 16) nên các DAG Run có thể chạy song song; `schedule_interval` đã deprecated; run_id dài làm tràn `batch_id` (64 ký tự) | Đọc code | `max_active_runs=1`, `schedule=`, rút gọn `batch_id` bằng hash |
| G4 | Cao | Docker | Tạo DB bằng `sleep 5` + psql không bảo đảm chạy sau khi initdb xong; healthcheck `pg_isready` báo healthy trước khi DB tồn tại; pipeline lỗi thì Airflow UI không khởi động | Đọc code (chưa chạy Docker) | `init-db.sql` qua `docker-entrypoint-initdb.d`; healthcheck kiểm tra DB `express_dwh`; pipeline lỗi vẫn bật UI |
| V1 | TB | `01_create_views.sql` | Không có chỉ số đơn đang mở đã quá hạn; `CREATE OR REPLACE VIEW` không cho chèn cột mới vào giữa | Đọc code | `overdue_open_shipments` tính tại thời điểm truy vấn; `DROP VIEW` trước khi tạo lại |

### 10.3 Kết quả đo trước / sau (một lần chạy mẫu; số liệu dao động nhẹ vì dữ liệu sinh theo `now()`)

| Phép đo | Bản gốc | Bản đã sửa |
|---|---|---|
| Timestamp ở tương lai trong OLTP (shipment / event / COD) | 4 / 4 / 12 | 0 / 0 / 0 |
| Đơn DELIVERED không có sự kiện DELIVERED / mang status IN_TRANSIT trên Fact | 1.592 / 1.592 | 0 / 0 |
| Bản ghi con mất không dấu vết (event / COD / ticket) | 836 / 78 / 13 | 0 / 0 / 0 |
| Lần chạy 2: số shipment / COD được extract | 0 / 0 | 250 / 116 (50 đơn giao + 200 đơn mới / 40 quyết toán + COD của đơn mới) |
| Khách đổi hạng thật ở lần chạy 2 | 15 / 25 | 25 / 25 |
| Đơn RETURNED bị tính lead time / vi phạm SLA | 791 / 388 | 0 / 0 |
| `on_time + sla_breached = delivered` | Sai (6.544 + 1.492 ≠ 7.648) | Đúng (6.568 + 1.202 = 7.770) |
| Tổng `pending_delivery_count` | 0 | ≈ 6.250 |
| Đơn cân lại (weight / freight / hub đích) trên Gold | Giữ giá trị cũ | Cập nhật đúng |
| Đơn Blocker được sửa ở nguồn | Lên Gold với 0 event | Lên Gold đủ sự kiện và các mốc thời gian |
| Event đến muộn | Trùng `event_sequence` | Thứ tự liên tục |
| Đơn `DELIVERY_FAILED` / đơn thiếu người nhận | Bị cách ly / sập batch | Lên Gold (receiver SK = -1) |
| Điều chuyển shipper có hiệu lực lùi 10 ngày | — | Phiên bản mới bắt đầu từ ngày hiệu lực, 96 sự kiện được re-key, 0 Fact sai phiên bản |
| Dời `order_date` muộn hơn sự kiện / đơn giao trong tương lai | — | Đều bị chặn Blocker; Silver và Gold giữ bản hợp lệ |
| Hai batch chạy chồng | Không chặn | Batch sau bị chặn, 0 dòng được ghi |
| Batch lỗi giữa chừng | Kẹt EXTRACTED | FAILED; batch kế tiếp chạy bình thường |
| Chạy lại cùng `batch_id` | — | Checksum Gold / Silver không đổi |

### 10.4 Lưu ý khi áp dụng bản sửa

* **Bắt buộc dựng lại dữ liệu.** DDL dùng `IF NOT EXISTS` nên DB cũ không nhận các cột mới (`silver.couriers.effective_date`, `fact_shipment_lifecycle.shipment_id_nk`) và không bỏ được các NOT NULL ở Silver. Dữ liệu OLTP cũ cũng còn timestamp tương lai và timeline sai. Với Docker: `docker-compose down -v`. Chạy local: xem Cách 1b ở mục 9.
* Lookback 10 phút: mỗi batch đọc lại các dòng thay đổi trong 10 phút trước snapshot của batch trước, nên `rows_extracted` có thể lớn hơn số thay đổi thật và Warning có thể lặp ở 2 batch liền nhau. Đây là đánh đổi có chủ đích.
* Ý nghĩa một số cột đã đổi: `pending_delivery_count`, `is_sla_breached`, `final_delivery_timestamp` / lead time (chỉ tính đơn DELIVERED), `delivery_attempt_count` (không đếm RETURNED). Dữ liệu sinh mới có thêm sự kiện `DELIVERY_ATTEMPT` và đơn ở trạng thái `CREATED`. Số liệu dashboard sẽ thay đổi theo hướng đúng hơn.

### 10.5 Giới hạn thiết kế còn lại (không phải lỗi; muốn khắc phục cần đổi công nghệ)

* CDC dựa trên `updated_at` không bắt được lệnh **DELETE** cứng ở nguồn và giao dịch OLTP chạy lâu hơn cửa sổ lookback. Muốn bắt tuyệt đối cần CDC đọc WAL (logical replication / Debezium) hoặc quy ước soft-delete. Hiện OLTP có FK nên việc xóa cứng vận đơn / sự kiện gần như không xảy ra.
* Khi `effective_date` ở tương lai (điều chuyển đã lên lịch), phiên bản mới mang `is_current = TRUE` dù chưa tới ngày hiệu lực. Phân tích theo thời điểm qua `valid_from` / `valid_to` vẫn đúng.

### 10.6 Truy vấn đối soát nhanh (kỳ vọng mọi cột = 0, riêng truy vấn 4 kỳ vọng 2 cột bằng nhau)

```sql
-- 1. Không còn bản ghi "mất im lặng": có ở stg, không có ở silver và cũng không bị Blocker
SELECT COUNT(*) AS mat_im_lang
FROM stg.shipment_tracking_events e
WHERE NOT EXISTS (SELECT 1 FROM silver.shipment_tracking_events s WHERE s.event_id = e.event_id)
  AND NOT EXISTS (
      SELECT 1 FROM meta.data_quarantine q
      WHERE q.source_table = 'shipment_tracking_events' AND q.severity = 'Blocker'
        AND q.record_id = e.event_id::TEXT
  );

-- 2. Trạng thái hiện tại trên Fact khớp với cờ nghiệp vụ
SELECT COUNT(*) AS lech_trang_thai
FROM gold.fact_shipment_lifecycle f
JOIN gold.dim_status_reason sr ON sr.status_reason_sk = f.current_status_sk
WHERE (f.is_delivered AND sr.status_code <> 'DELIVERED')
   OR (f.is_returned  AND sr.status_code <> 'RETURNED')
   OR (f.is_cancelled AND sr.status_code <> 'CANCELLED');

-- 3. Mọi Fact trỏ đúng phiên bản SCD2 bao trùm thời điểm phát sinh
SELECT
    (SELECT COUNT(*) FROM gold.fact_tracking_events e
     JOIN gold.dim_courier c ON c.courier_sk = e.courier_sk
     WHERE NOT (e.event_timestamp >= c.valid_from AND e.event_timestamp < c.valid_to)) AS su_kien_sai_phien_ban,
    (SELECT COUNT(*) FROM gold.fact_shipment_lifecycle f
     JOIN gold.dim_customer c ON c.customer_sk IN (f.sender_customer_sk, f.receiver_customer_sk)
     WHERE NOT (f.order_timestamp >= c.valid_from AND f.order_timestamp < c.valid_to)) AS van_don_sai_phien_ban;

-- 4. Đơn đã giao: đúng hạn + trễ hạn = tổng đã giao
SELECT COUNT(*) FILTER (WHERE is_delivered) AS delivered,
       COUNT(*) FILTER (WHERE is_on_time) + COUNT(*) FILTER (WHERE is_sla_breached) AS on_time_plus_breached
FROM gold.fact_shipment_lifecycle;
```