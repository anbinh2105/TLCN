# Data Source Catalog

## 1. Overview

Tài liệu này mô tả các nguồn dữ liệu đầu vào được sử dụng trong đề tài:

**“Xây dựng Data Lakehouse phục vụ phân tích và dự đoán nguy cơ ngập lụt tại khu vực trung tâm Cần Thơ.”**

Mục tiêu của Data Source Catalog là:

- Kiểm kê các nguồn dữ liệu đầu vào.
- Mô tả định dạng và cấu trúc dữ liệu.
- Xác định vai trò của từng dataset trong bài toán.
- Xác định business key.
- Xác định độ phân giải thời gian (temporal resolution).
- Ghi nhận các vấn đề chất lượng dữ liệu ban đầu.
- Xác định canonical schema mà Silver Layer sẽ hướng tới.
- Làm cơ sở xây dựng các bước ingestion, data quality, transformation và feature engineering sau này.

> Lưu ý: Data Source Catalog mô tả dữ liệu nguồn và schema mục tiêu.  
> Các phép làm sạch, chuẩn hóa, khôi phục dữ liệu hoặc nội suy không được thực hiện tại bước khảo sát nguồn.

---

# 2. Source Inventory

## 2.1 Tổng quan

Hệ thống hiện sử dụng 5 nhóm dữ liệu chính:

| Dataset | Vai trò | Temporal Resolution | Số file | Format | Dung lượng |
|---|---|---:|---:|---|---:|
| Weather | Feature | 1 giờ | 1 | CSV | 2.52 MB |
| Rainfall | Feature | 1 giờ | 1 | CSV | 1.80 MB |
| Water Level | Target chính + Feature | 15 phút | 1 | XLSX | 10.00 MB |
| River Discharge | Feature | 1 ngày | 1 | CSV | 74.22 KB |
| Tide | Feature | 1 giờ | 60 | XLSX + CSV | 1.11 MB |
| **Total** | | | **64** | | **15.49 MB** |

Các dataset có temporal resolution khác nhau. Vì vậy dữ liệu sẽ được giữ ở độ phân giải gốc tại Silver Layer và chỉ được đồng bộ về temporal grain chung khi xây dựng Gold Layer phục vụ phân tích và mô hình dự báo.

---

# 3. Weather Dataset

## 3.1 General Information

| Thuộc tính | Giá trị |
|---|---|
| Dataset | `weather` |
| Vai trò | Feature |
| Source file | `weather_can_tho_2018_2026.csv` |
| Format | CSV |
| Số file | 1 |
| Dung lượng | 2.52 MB |
| Temporal resolution | 1 giờ |
| Update mode hiện tại | Historical batch |
| Business key dự kiến | `timestamp` |

## 3.2 Raw Schema

Schema nguồn hiện tại:

| Column | Ý nghĩa dự kiến |
|---|---|
| `timestamp` | Thời điểm quan trắc |
| `temperature` | Nhiệt độ |
| `humidity` | Độ ẩm |
| `pressure` | Áp suất |
| `wind_speed` | Tốc độ gió |
| `wind_direction` | Hướng gió |
| `latitude` | Vĩ độ |
| `longitude` | Kinh độ |
| `date` | Ngày dẫn xuất |
| `year` | Năm dẫn xuất |
| `month` | Tháng dẫn xuất |
| `day` | Ngày dẫn xuất |
| `hour` | Giờ dẫn xuất |

Các cột `date`, `year`, `month`, `day`, `hour` là các thuộc tính thời gian dẫn xuất và không được sử dụng để xác định business key.

## 3.3 Data Profile

Các chỉ số sau sẽ được cập nhật sau bước profiling:

| Metric | Value |
|---|---|
| Record count | TBD |
| Start timestamp | TBD |
| End timestamp | TBD |
| Missing timestamp | TBD |
| Missing values | TBD |
| Duplicate business keys | TBD |
| Temperature min/max | TBD |
| Humidity min/max | TBD |
| Pressure min/max | TBD |
| Wind speed min/max | TBD |

## 3.4 Canonical Schema

Silver Layer dự kiến chuẩn hóa Weather thành:

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp` | timestamp | - | Thời điểm quan trắc |
| `temperature_c` | double | °C | Nhiệt độ |
| `humidity_pct` | double | % | Độ ẩm |
| `pressure_hpa` | double | hPa | Áp suất |
| `wind_speed` | double | TBD | Tốc độ gió |
| `wind_direction` | double | TBD | Hướng gió |
| `latitude` | double | degree | Vĩ độ |
| `longitude` | double | degree | Kinh độ |
| `quality_flag` | string | - | Trạng thái chất lượng dữ liệu |

---

# 4. Rainfall Dataset

## 4.1 General Information

| Thuộc tính | Giá trị |
|---|---|
| Dataset | `rainfall` |
| Vai trò | Feature |
| Source file | `rainfall_can_tho_2018_2026.csv` |
| Format | CSV |
| Số file | 1 |
| Dung lượng | 1.80 MB |
| Temporal resolution | 1 giờ |
| Update mode hiện tại | Historical batch |
| Business key dự kiến | `timestamp` |

## 4.2 Raw Schema

| Column | Ý nghĩa dự kiến |
|---|---|
| `timestamp` | Thời điểm quan trắc |
| `rainfall_mm` | Lượng mưa |
| `latitude` | Vĩ độ |
| `longitude` | Kinh độ |
| `date` | Ngày dẫn xuất |
| `year` | Năm dẫn xuất |
| `month` | Tháng dẫn xuất |
| `day` | Ngày dẫn xuất |
| `hour` | Giờ dẫn xuất |

## 4.3 Data Profile

| Metric | Value |
|---|---|
| Record count | TBD |
| Start timestamp | TBD |
| End timestamp | TBD |
| Missing timestamp | TBD |
| Missing rainfall | TBD |
| Missing % | TBD |
| Duplicate business keys | TBD |
| Rainfall min | TBD |
| Rainfall max | TBD |

## 4.4 Canonical Schema

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp` | timestamp | - | Thời điểm quan trắc |
| `rainfall_mm` | double | mm | Lượng mưa |
| `latitude` | double | degree | Vĩ độ |
| `longitude` | double | degree | Kinh độ |
| `quality_flag` | string | - | Trạng thái chất lượng |

---

# 5. Water Level Dataset

## 5.1 General Information

| Thuộc tính | Giá trị |
|---|---|
| Dataset | `water_level` |
| Vai trò | **Target chính + Feature** |
| Source file | `water_level_can_tho_2018_2026.xlsx` |
| Format | XLSX |
| Số file | 1 |
| Dung lượng | 10.00 MB |
| Temporal resolution | 15 phút |
| Update mode hiện tại | Historical batch |
| Business key dự kiến | `station_code + timestamp` |

Water Level là dataset trung tâm của bài toán. Giá trị mực nước tương lai sẽ được sử dụng làm target cho các mô hình forecasting.

## 5.2 Raw Schema

| Column |
|---|
| `Country Code` |
| `Station Code` |
| `Parameter` |
| `Label` |
| `Timestamp (UTC+07:00)` |
| `Value` |
| `Unit` |
| `Observed or Estimated` |
| `Approval Level` |
| `Grade` |

Timestamp của nguồn được biểu diễn theo UTC+07:00.

## 5.3 Data Profile

| Metric | Value |
|---|---|
| Record count | TBD |
| Start timestamp | TBD |
| End timestamp | TBD |
| Station count | TBD |
| Missing timestamp | TBD |
| Missing water level | TBD |
| Missing % | TBD |
| Duplicate business keys | TBD |
| Water level min | TBD |
| Water level max | TBD |

## 5.4 Canonical Schema

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp` | timestamp | - | Thời điểm quan trắc |
| `station_code` | string | - | Mã trạm |
| `water_level_m` | double | m | Mực nước |
| `unit` | string | - | Đơn vị nguồn |
| `quality_flag` | string | - | Trạng thái chất lượng |
| `source_file` | string | - | File nguồn |

Business key:

```text
station_code + timestamp
```

Việc sử dụng `station_code` cùng `timestamp` cho phép schema tiếp tục hoạt động nếu sau này bổ sung dữ liệu từ nhiều trạm.

---

# 6. River Discharge Dataset

## 6.1 General Information

| Thuộc tính | Giá trị |
|---|---|
| Dataset | `river_discharge` |
| Vai trò | Feature |
| Source file | `river_discharge_can_tho_2018_2026.csv` |
| Format | CSV |
| Số file | 1 |
| Dung lượng | 74.22 KB |
| Temporal resolution | 1 ngày |
| Update mode hiện tại | Historical batch |
| Business key dự kiến | `station + timestamp` |

River Discharge có temporal resolution thấp hơn các nguồn còn lại. Silver Layer sẽ giữ nguyên dữ liệu daily. Việc chuyển đổi để kết hợp với dữ liệu hourly sẽ được thực hiện ở Gold Layer theo chiến lược temporal alignment được xác định sau.

## 6.2 Raw Schema

| Column | Ý nghĩa dự kiến |
|---|---|
| `timestamp` | Thời gian |
| `station` | Trạm |
| `discharge_m3s` | Lưu lượng sông |
| `date` | Ngày dẫn xuất |
| `year` | Năm dẫn xuất |
| `month` | Tháng dẫn xuất |

## 6.3 Data Profile

| Metric | Value |
|---|---|
| Record count | TBD |
| Start timestamp | TBD |
| End timestamp | TBD |
| Station count | TBD |
| Missing discharge | TBD |
| Missing % | TBD |
| Duplicate business keys | TBD |
| Discharge min | TBD |
| Discharge max | TBD |

## 6.4 Canonical Schema

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp` | timestamp | - | Thời điểm quan trắc |
| `station` | string | - | Trạm |
| `discharge_m3s` | double | m³/s | Lưu lượng |
| `quality_flag` | string | - | Trạng thái chất lượng |

Business key:

```text
station + timestamp
```

---

# 7. Tide Dataset

## 7.1 General Information

| Thuộc tính | Giá trị |
|---|---|
| Dataset | `tide` |
| Vai trò | Feature |
| Directory | `data/raw/tide/` |
| Số file | 60 |
| XLSX files | 36 |
| CSV files | 24 |
| Dung lượng | 1.11 MB |
| Temporal resolution | 1 giờ |
| Update mode hiện tại | Historical batch |
| Business key dự kiến | `timestamp` |

Tide là nguồn có cấu trúc file phức tạp nhất trong 5 dataset vì dữ liệu được chia theo tháng và tồn tại đồng thời hai định dạng XLSX và CSV.

Inventory hiện tại có:

```text
2021: 12 monthly files
2022: 12 monthly files
2023: 12 monthly files
2024: 12 monthly files
2025: 12 monthly files
```

Tổng:

```text
5 years × 12 months = 60 files
```

Trong đó:

```text
2021–2023 → XLSX
2024–2025 → CSV
```

## 7.2 Raw Schema

Schema logic của Tide:

| Column | Description |
|---|---|
| `Ngày` | Ngày quan trắc |
| `Giờ` | Giờ quan trắc |
| `Độ cao (m)` | Độ cao triều |

Các file XLSX và CSV có thể khác nhau về cách biểu diễn vật lý của giá trị và delimiter, nhưng đại diện cho cùng một schema logic.

## 7.3 Known Source Characteristics

Trong quá trình kiểm tra dữ liệu nguồn trước khi xây dựng lại pipeline, Tide đã cho thấy một số vấn đề cần được kiểm tra tự động trong bước Data Quality:

- Nhiều file XLSX chứa giá trị `Độ cao (m)` có representation không đồng nhất.
- Một số cell có thể được thư viện đọc dưới dạng Excel date/datetime thay vì numeric.
- Một số giá trị khác được lưu dưới dạng string hoặc numeric.
- CSV giữa các năm có thể sử dụng delimiter khác nhau.
- Dữ liệu cần được kiểm tra temporal completeness theo từng tháng.
- Không được giả định file có đủ số observation chỉ dựa vào tên file.

Các vấn đề đã được xác minh trong source trước khi xây dựng lại pipeline cần được ghi lại trong correction/audit log riêng nếu source được hiệu chỉnh.

## 7.4 Data Profile

| Metric | Value |
|---|---|
| Total record count | TBD |
| Start timestamp | TBD |
| End timestamp | TBD |
| Missing timestamp count | TBD |
| Missing tide values | TBD |
| Duplicate timestamps | TBD |
| Conflicting duplicates | TBD |
| Tide min | TBD |
| Tide max | TBD |
| Expected hourly completeness | TBD |

Ngoài profiling toàn dataset, Tide cần được profile theo từng `source_file` để phát hiện:

- tháng thiếu observation;
- timestamp nằm ngoài tháng được khai báo;
- duplicate timestamp;
- overlap giữa các monthly files;
- sai năm/tháng;
- representation bất thường của `Độ cao (m)`.

## 7.5 Canonical Schema

| Column | Type | Unit | Description |
|---|---|---|---|
| `timestamp` | timestamp | - | Timestamp được tạo từ Ngày + Giờ |
| `tide_m` | double | m | Độ cao triều |
| `quality_flag` | string | - | Trạng thái chất lượng |
| `recovery_method` | string | - | Phương pháp recovery nếu cần |
| `source_file` | string | - | File nguồn |

Business key:

```text
timestamp
```

`source_file` được giữ lại nhằm hỗ trợ lineage và truy vết lỗi về monthly file ban đầu.

---

# 8. Business Keys

Business key được sử dụng để kiểm tra duplicate và tính duy nhất của observation.

| Dataset | Business Key |
|---|---|
| Weather | `timestamp` |
| Rainfall | `timestamp` |
| Water Level | `station_code + timestamp` |
| River Discharge | `station + timestamp` |
| Tide | `timestamp` |

Các thuộc tính dẫn xuất như:

```text
date
year
month
day
hour
```

không được sử dụng làm business key.

---

# 9. Temporal Characteristics

Các nguồn dữ liệu có độ phân giải thời gian khác nhau:

| Dataset | Native Temporal Resolution | Silver Resolution dự kiến |
|---|---:|---:|
| Water Level | 15 phút | 15 phút |
| Rainfall | 1 giờ | 1 giờ |
| Weather | 1 giờ | 1 giờ |
| Tide | 1 giờ | 1 giờ |
| River Discharge | 1 ngày | 1 ngày |

Silver Layer ưu tiên bảo toàn temporal resolution gốc.

Không thực hiện resampling tất cả dataset về cùng một frequency tại Silver.

Gold Layer sau này sẽ xây dựng dataset chung ở mức:

```text
1 hour
```

để phục vụ:

- phân tích;
- visualization;
- feature engineering;
- forecasting water level;
- flood-risk analysis.

---

# 10. Dataset Roles in Forecasting

Bài toán chính được định hướng là dự báo mực nước tương lai.

Có thể biểu diễn tổng quát:

```text
Rainfall ───────────┐
Weather ────────────┤
Tide ───────────────┤
River Discharge ────┼──► Forecasting Model ───► Future Water Level
Historical WL ──────┘
```

Vai trò:

| Dataset | Modeling Role |
|---|---|
| Water Level | Main target + historical feature |
| Rainfall | Exogenous feature |
| Weather | Exogenous feature |
| Tide | Exogenous feature |
| River Discharge | Exogenous feature |

Việc lựa chọn feature cuối cùng sẽ được đánh giá trong giai đoạn phân tích và modeling, không được quyết định chỉ dựa trên việc feature tồn tại trong nguồn.

---

# 11. Canonical Naming Convention

Silver Layer sử dụng quy tắc:

```text
lowercase_snake_case
```

Ví dụ:

```text
Timestamp (UTC+07:00)
        ↓
timestamp

Station Code
        ↓
station_code

Value
        ↓
water_level_m

Độ cao (m)
        ↓
tide_m
```

Tên canonical cần thể hiện rõ ý nghĩa và đơn vị khi phù hợp:

```text
rainfall_mm
water_level_m
tide_m
discharge_m3s
temperature_c
humidity_pct
pressure_hpa
```

---

# 12. Data Quality Dimensions

Các dataset sẽ được đánh giá theo các nhóm chất lượng sau.

### Completeness

Kiểm tra:

- null;
- missing observation;
- missing timestamp;
- temporal gap.

### Validity

Kiểm tra:

- timestamp parseable;
- numeric parseable;
- required columns;
- datatype;
- unit;
- giá trị nằm ngoài phạm vi hợp lý.

### Uniqueness

Kiểm tra:

- duplicate business key;
- exact duplicate;
- conflicting duplicate.

### Consistency

Kiểm tra:

- schema giữa các file;
- format;
- delimiter;
- unit;
- temporal resolution;
- timestamp consistency.

### Traceability

Các record sau transformation cần có khả năng truy vết về nguồn thông qua các metadata phù hợp như:

```text
source_file
quality_flag
recovery_method
```

---

# 13. Raw-to-Canonical Mapping

| Dataset | Raw Field | Canonical Field |
|---|---|---|
| Weather | `timestamp` | `timestamp` |
| Weather | `temperature` | `temperature_c` |
| Weather | `humidity` | `humidity_pct` |
| Weather | `pressure` | `pressure_hpa` |
| Weather | `wind_speed` | `wind_speed` |
| Weather | `wind_direction` | `wind_direction` |
| Rainfall | `timestamp` | `timestamp` |
| Rainfall | `rainfall_mm` | `rainfall_mm` |
| Water Level | `Timestamp (UTC+07:00)` | `timestamp` |
| Water Level | `Station Code` | `station_code` |
| Water Level | `Value` | `water_level_m` |
| Water Level | `Unit` | `unit` |
| River Discharge | `timestamp` | `timestamp` |
| River Discharge | `station` | `station` |
| River Discharge | `discharge_m3s` | `discharge_m3s` |
| Tide | `Ngày` + `Giờ` | `timestamp` |
| Tide | `Độ cao (m)` | `tide_m` |

---

# 14. Current Profiling Status

## Completed

- [x] Xác định 5 nhóm data sources.
- [x] Kiểm kê toàn bộ source files.
- [x] Xác định file formats.
- [x] Xác định dung lượng source.
- [x] Xác định vai trò dự kiến của từng dataset.
- [x] Xác định business key dự kiến.
- [x] Xác định native temporal resolution.
- [x] Xây dựng canonical schema ban đầu.

## Pending

- [ ] Record count.
- [ ] Actual timestamp coverage.
- [ ] Actual datatype profiling.
- [ ] Missing count và missing percentage.
- [ ] Duplicate business keys.
- [ ] Numeric min/max.
- [ ] Temporal gap.
- [ ] Tide monthly completeness.
- [ ] Cross-file overlap.
- [ ] Unit validation.
- [ ] Final canonical schema validation.

---

# 15. Step 1 Completion Criteria

Bước 1 — Data Source Profiling chỉ được xem là hoàn thành khi mỗi dataset có đầy đủ:

```text
Dataset name
Source
Format
File count
Record count
Schema
Business key
Temporal resolution
Temporal coverage
Unit
Missing count / %
Duplicate count
Numeric min/max
Known data-quality issues
Canonical schema
```

Kết quả của bước này sẽ được sử dụng làm đầu vào cho:

```text
Step 2 — Batch Ingestion
            ↓
Step 3 — Bronze Layer
            ↓
Step 4 — Bronze Data Quality Gate
            ↓
Step 5 — Silver Transformation
```