Dựa vào danh sách "Top 5 Lakehouse Anti-Patterns", lỗi mà team chúng tôi dễ vướng phải nhất trong thực tế là **Anti-pattern #3: "Bỏ qua OPTIMIZE → small-file problem"**.

**Vì sao team dễ vướng phải?**
Trong các dự án thực tế, dữ liệu stream thường được đổ về liên tục (micro-batch) để đạt độ trễ thấp. Áp lực release tính năng khiến team thường chỉ tập trung vào luồng ingest ghi data thành công, nhưng lại quên mất khâu dọn dẹp và bảo trì (maintenance).

**Hậu quả:**
Cứ mỗi lần append lại đẻ ra một file winrar vài chục KB. Theo thời gian, hệ thống tích tụ hàng chục ngàn file nhỏ lẻ (ví dụ "10K files x 1 MB"). Thay vì scan một file lớn, hệ thống mất rất nhiều IO overhead để mở từng file lẻ, khiến tốc độ query "chậm đi 10x". Vấn đề này đã được thể hiện rõ ở Notebook `02_optimize_zorder`.

**Cách khắc phục:**
Chúng tôi cần thiết lập thói quen xây dựng các job bảo trì ngầm, cụ thể là lập lịch chạy lệnh `OPTIMIZE` định kỳ (daily cronjob) ngay từ giai đoạn thiết kế ban đầu để đảm bảo hệ thống không bị "phình to" rác theo thời gian.
