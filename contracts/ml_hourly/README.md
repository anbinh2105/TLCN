# ML Data Product — Can Tho Flood Risk Forecasting

## 1. Tổng quan

Data Product `iceberg.gold.ml_hourly` cung cấp dữ liệu thủy văn và khí tượng được chuẩn hóa, tích hợp theo giờ để phục vụ bài toán dự báo mực nước tại khu vực trung tâm Cần Thơ.

- Producer: Data Engineering
- Consumer: Machine Learning
- Format: Apache Iceberg / Parquet
- Granularity: 1 giờ × 1 địa điểm
- Tổng bản ghi tại thời điểm bàn giao: 73.656
- Iceberg Snapshot ID: `2528388719354325991`

## 2. Nhóm dữ liệu

- Water Level: mean, min, max, last, observation count.
- Rainfall: hourly rainfall.
- Tide: tide height và recovery metadata.
- River Discharge: lưu lượng nguồn ngày được căn chỉnh lên giờ.
- Weather: nhiệt độ, độ ẩm, áp suất, tốc độ và hướng gió.
- Data Quality: present, usable và quality flags.

## 3. Quy tắc sử dụng

- Không thay NULL bằng 0 khi chưa có chiến lược xử lý dữ liệu thiếu.
- Chỉ tạo target từ những quan trắc mực nước hợp lệ.
- Chia Train/Validation/Test theo thứ tự thời gian.
- Không sử dụng dữ liệu quan trắc tương lai làm input.
- Không sử dụng `gold_processed_at` làm feature dự báo.
- Không trộn dữ liệu streaming mô phỏng vào tập huấn luyện lịch sử.
- Lưu lại Iceberg snapshot ID để bảo đảm khả năng tái lập thí nghiệm.

## 4. Machine Learning Pipeline

`gold.ml_hourly` → PySpark Feature Engineering → Lag/Rolling/Lookback → Forecast Targets → Train/Validation/Test → Model Training → MLflow.

## 5. Giới hạn

Các ngưỡng 1,8 m, 1,9 m và 2,0 m chỉ phục vụ so sánh tham khảo. Hệ quy chiếu cao độ của dữ liệu mực nước chưa được xác minh. Kết quả không được trình bày là cảnh báo ngập chính thức.

## 6. Trạng thái bàn giao

Status: VALIDATED

Contract version: 1.0.0

Data Product: iceberg.gold.ml_hourly

Iceberg Snapshot ID: 2528388719354325991

Validation result: 14 PASS / 0 FAIL

Total records: 73,656

Validation date: 2026-10-15

Dữ liệu đã vượt qua các kiểm thử về schema, primary key, timestamp alignment, quality flags, tính nhất quán với Gold nguồn và Iceberg snapshot.

Thành viên Machine Learning có thể sử dụng Data Product này làm dữ liệu đầu vào cho PySpark Feature Engineering, tạo các đặc trưng lag/rolling, xây dựng nhãn dự báo và huấn luyện mô hình.

Lưu ý: Dữ liệu NULL phải được xử lý có chủ đích; không sử dụng quan trắc tương lai làm input; lưu lượng theo giờ có nguồn gốc từ dữ liệu ngày; ngưỡng cảnh báo mực nước chỉ phục vụ so sánh tham khảo khi hệ quy chiếu cao độ chưa được xác minh.

## 7. Hướng dẫn truy cập dữ liệu

### Truy vấn bằng Trino

Data Product được lưu trong bảng:

`iceberg.gold.ml_hourly`

Ví dụ truy vấn:

```sql
SELECT
    hour_start,
    location_id,
    water_level_mean,
    rainfall_mm,
    tide_height_m,
    discharge_m3s,
    temperature_c,
    water_level_usable
FROM iceberg.gold.ml_hourly
ORDER BY hour_start
LIMIT 100;
```

### Đọc bằng PySpark

Trong môi trường Spark đã được cấu hình Iceberg Catalog:

```python
df = spark.table("lakehouse.gold.ml_hourly")

df.printSchema()
df.show(5, truncate=False)
```

Tên catalog `lakehouse` cần được xác nhận theo cấu hình Spark của thành viên ML.

### Quy định chất lượng dữ liệu

- `*_present = true`: nguồn dữ liệu có bản ghi tại thời điểm tương ứng.
- `*_usable = true`: dữ liệu đáp ứng quy tắc chất lượng để sử dụng.
- `*_quality_flag`: mô tả trạng thái hoặc nguyên nhân liên quan đến chất lượng dữ liệu.
- `all_five_usable = true`: cả năm nhóm dữ liệu đều usable tại cùng thời điểm.

Không được mặc định rằng dữ liệu có mặt (`present`) thì luôn đủ điều kiện sử dụng (`usable`).

### Quy định thời gian

`hour_start` được lưu dưới dạng timestamp có timezone. Khi xử lý, cần diễn giải theo múi giờ `Asia/Ho_Chi_Minh` (UTC+7).

Các phép tổng hợp theo giờ phải được sử dụng sau khi khoảng giờ tương ứng đã kết thúc và dữ liệu đã sẵn sàng.

### Quy định tái lập thí nghiệm

Thành viên Machine Learning phải lưu Iceberg Snapshot ID và phiên bản Data Contract tương ứng với mỗi lần huấn luyện để có thể tái lập kết quả.