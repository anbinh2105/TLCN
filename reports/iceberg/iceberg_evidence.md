# BƯỚC 7.7 — EVIDENCE KIỂM CHỨNG APACHE ICEBERG

**Đề tài:** Xây dựng Data Lakehouse phục vụ phân tích và dự đoán nguy cơ ngập lụt tại khu vực trung tâm Cần Thơ  
**Ngày tổng hợp:** 07/10/2026  
**Nền tảng:** Apache Spark 4.1.1, Iceberg 1.12.0, Hive Metastore, MinIO.  
**Phạm vi:** Các bảng thử nghiệm `lakehouse.demo`, nguồn dữ liệu Silver Weather chỉ đọc.

## 1. Mục tiêu và phương pháp

Kiểm chứng thực nghiệm các khả năng quản lý bảng, snapshot, truy vấn lịch sử, thay đổi schema, cập nhật có commit, truy vấn metadata và phân vùng theo thời gian của Apache Iceberg. Dữ liệu thí nghiệm được tách khỏi các bảng nghiệp vụ Silver/Gold. Bằng chứng là các **log terminal gốc đã được người thực hiện tải lên cuộc trò chuyện**; bản sao lưu tại `logs/` không phải kết quả chạy lại. Tên file có `partial` biểu thị lần chạy ban đầu dừng ở bước xác minh tiếp theo, không đồng nghĩa thao tác ghi đã thất bại.

## 2. Kết quả theo từng capability

### 7.1 — Khởi tạo bảng Iceberg
- **Mục tiêu:** Xác minh có thể tạo bảng Iceberg từ dữ liệu Silver.
- **Thao tác:** Tạo bảng `lakehouse.demo.weather_iceberg_demo` từ tập con Weather.
- **Kết quả:** Bảng ban đầu có 100 bản ghi, 12 cột, 1 snapshot.
- **Nhận xét:** Đã tạo được bảng thử nghiệm và ghi nhận snapshot đầu tiên.
- **Log:** `logs/01_setup_20261006.txt`.

### 7.2 — Snapshot Versioning
- **Mục tiêu:** Quan sát thay đổi phiên bản khi append dữ liệu.
- **Thao tác:** Append một bản ghi demo vào bảng đã tạo.
- **Kết quả:** 100 → 101 bản ghi; 1 → 2 snapshots. Snapshot 1: `4064462945497131262`; Snapshot 2: `8625072115902793184`, parent là Snapshot 1, operation `append`.
- **Nhận xét:** Có thể theo dõi lịch sử commit dữ liệu.
- **Log:** `logs/02_snapshot_20261006.txt`.

### 7.3 — Time Travel
- **Mục tiêu:** Truy vấn các trạng thái lịch sử.
- **Thao tác:** Đọc theo Snapshot 1 và Snapshot 2.
- **Kết quả:** Snapshot 1 có 100 bản ghi; Snapshot 2 có 101 bản ghi. Truy vấn không tạo snapshot mới.
- **Nhận xét:** Chứng minh khả năng đọc dữ liệu theo phiên bản trong phạm vi thử nghiệm.
- **Log:** `logs/03_time_travel_20261006.txt`.

### 7.4 — Schema Evolution
- **Mục tiêu:** Thay đổi cấu trúc bảng mà không làm mất dữ liệu hiện có.
- **Thao tác:** `ALTER TABLE ... ADD COLUMN demo_note STRING`.
- **Kết quả:** 12 → 13 cột; vẫn 101 bản ghi; số data snapshots giữ nguyên 2.
- **Nhận xét:** Đây là thay đổi metadata, không cần tạo thêm data snapshot.
- **Log:** `logs/04_schema_evolution_20261006.txt`.

### 7.5 — ACID / Commit
- **Mục tiêu:** Kiểm tra cập nhật bản ghi thử nghiệm được commit và có phiên bản lịch sử.
- **Thao tác:** `UPDATE` một bản ghi demo để gán `demo_note='ACID_COMMIT_VERIFIED'`; chạy kiểm chứng READ ONLY.
- **Kết quả:** 101 bản ghi không đổi; 2 → 3 snapshots; Snapshot 3 `7829982116285418825`, operation `overwrite`; đọc được trạng thái lịch sử.
- **Nhận xét:** Có bằng chứng cho atomic commit và versioned update trong thử nghiệm này; **không** tự động chứng minh mọi thuộc tính ACID dưới tải đồng thời.
- **Log:** `logs/05_acid_update_partial_20261006.txt` (lần đầu có lỗi ở phần đọc lịch sử sau commit), `logs/06_acid_readonly_verified_20261007.txt` (PASS).

### 7.6A — Metadata Management
- **Mục tiêu:** Kiểm tra metadata files, manifests, snapshots, lineage và references.
- **Thao tác:** Truy vấn metadata tables ở chế độ READ ONLY.
- **Kết quả:** 101 bản ghi, 13 cột, 2 data files, 3 manifests, 3 snapshots; main snapshot `7829982116285418825` không đổi.
- **Nhận xét:** Xác minh metadata có thể truy vấn độc lập với dữ liệu nghiệp vụ.
- **Log:** `logs/07_metadata_readonly_20261007.txt`.

### 7.6B — Partitioning và Timezone
- **Mục tiêu:** Tạo bảng phân vùng ngày và đối chiếu partition metadata với ngày hiển thị.
- **Thao tác:** CTAS `lakehouse.demo.weather_partitioned_demo` với `PARTITIONED BY (days(timestamp))`, sau đó kiểm tra READ ONLY.
- **Kết quả:** 168 bản ghi, 7 ngày địa phương (2018-04-25 đến 2018-05-01), 8 partitions Iceberg và 8 data files; Snapshot ID `8362217845622271219`.
- **Nhận xét:** Session timezone là `Asia/Ho_Chi_Minh`; bản ghi 00:00–06:00 giờ địa phương thuộc ngày UTC trước đó, 07:00–23:00 thuộc ngày UTC tương ứng. 8 partitions là kết quả của ranh giới ngày UTC, không phải mất hoặc nhân đôi dữ liệu.
- **Log:** `logs/08_partition_ctas_partial_20261007.txt` (CTAS thành công, kiểm tra dự kiến 7 partitions nên dừng), `logs/09_partition_readonly_verified_20261007.txt` (PASS với 8 partitions).

### 7.6B — Partition Filtering / Partition Pruning (ĐÃ KIỂM TRA, GIỚI HẠN ĐO LƯỜNG)
- **Mục tiêu:** Xác minh bộ lọc ngày được chuyển tới Iceberg scan và nhận diện các data files chứa bản ghi phù hợp.
- **Thao tác:** Chạy `verify_partition_pruning.py` ở chế độ READ ONLY, lọc ngày địa phương `2018-04-25` theo khoảng timestamp nửa mở `[2018-04-25 00:00, 2018-04-26 00:00)`; xem `EXPLAIN FORMATTED`, `_file`, `_partition` và snapshot.
- **Kết quả:** Bảng có 168 records / 8 data files; truy vấn trả về 24 records thuộc 2 partitions UTC (`2018-04-24`: 7 records; `2018-04-25`: 17 records); 2/8 data files **chứa bản ghi phù hợp**. `EXPLAIN FORMATTED` thể hiện `BatchScan` / `IcebergScan` và các điều kiện `timestamp >= ...` và `timestamp < ...` trong `filters`. Snapshot `8362217845622271219` không thay đổi.
- **Đánh giá:** **PASS — lọc dữ liệu và predicate pushdown được chứng minh**; metadata partition và file tương thích với truy vấn ngày. Kết quả 2/8 files là số file chứa bản ghi phù hợp, **không phải** số file Iceberg thực tế đã mở hoặc đọc được đo bằng scan metrics. Chưa chứng minh mức giảm I/O 75%, thời gian chạy hay tốc độ tăng hiệu năng. Để kết luận pruning thực tế loại bỏ chính xác 6 file khỏi scan cần bổ sung scan/task metrics hoặc bằng chứng scan planning tương đương.
- **Log:** `logs/10_partition_pruning_20261007.txt` (log terminal người dùng cung cấp, READ ONLY).

## 3. Bảng tổng hợp

| Bước | Capability | Trạng thái | Bằng chứng chính |
|---|---|---|---|
| 7.1 | Iceberg table creation | PASS | 100 records; 12 columns; initial snapshot |
| 7.2 | Snapshot versioning | PASS | Append 1 row; 100 -> 101; snapshots 1 -> 2 |
| 7.3 | Time travel | PASS | Snapshot 1 = 100; snapshot 2 = 101 |
| 7.4 | Schema evolution | PASS | Added demo_note; 12 -> 13 columns; 101 rows unchanged |
| 7.5 | ACID update / versioning | PASS (scope limited) | 1 demo record UPDATE committed; overwrite snapshot; 101 rows unchanged |
| 7.6A | Metadata management | PASS | 101 records; 2 data files; 3 manifests; 3 snapshots |
| 7.6B | Daily partitioning | PASS | 168 rows; 7 local days; 8 UTC partitions; 8 files |
| 7.6B | Partition filtering / predicate pushdown | PASS (scope limited) | 24 rows; 2/8 matching files; IcebergScan timestamp filters; unchanged snapshot; actual scan file count not measured |

## 4. Tính toàn vẹn và khả năng tái lập

- **Log trong thư mục `logs/`:** bản sao nguyên văn của những file log người thực hiện đã tải lên cuộc trò chuyện; không phải log vừa chạy lại.
- **Các script ghi dữ liệu:** không chạy lại `setup_iceberg_demo.py`, `demo_snapshot.py`, `demo_schema_evolution.py`, bản `demo_acid.py` có UPDATE, hay `demo_partitioned_table.py` có CTAS để tránh tạo thêm commit/snapshot hoặc xung đột bảng.
- **Chỉ nên chạy lại các script kiểm tra READ ONLY** khi cần chụp ảnh, xác minh trạng thái hoặc bổ sung phần pruning.
- **Các lỗi cảnh báo Spark:** cảnh báo xóa thư mục JAR tạm khi shutdown không phủ nhận kết quả PASS đã in trước đó; lỗi kiểm tra sau commit được giữ nguyên trong log, không xóa hoặc che giấu.
- **Không suy rộng:** bằng chứng thử nghiệm quy mô nhỏ chứng minh các chức năng đã quan sát, không chứng minh hiệu năng hay khả năng chịu tải sản xuất.

## 5. Kết luận

Bước 7 đã có bằng chứng cho khởi tạo bảng, snapshot versioning, time travel, schema evolution, commit cập nhật, metadata management và daily partitioning. Đã có execution plan xác nhận IcebergScan nhận bộ lọc timestamp, nhưng chưa có scan metrics để khẳng định số file thực sự đọc hoặc mức cải thiện hiệu năng. Sau khi hoàn thiện evidence, pipeline có thể tiếp tục bước 8 — Gold Temporal Alignment.
