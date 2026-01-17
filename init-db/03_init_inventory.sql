-- ==================================================================
-- SCRIPT KHỞI TẠO DỮ LIỆU TỒN KHO (INVENTORY)
-- ==================================================================
-- Mô tả: Tạo bản ghi tồn kho cho TẤT CẢ các cặp (Sản phẩm - Kho hàng)
-- Logic: Sử dụng CROSS JOIN giữa bảng product và warehouse

INSERT INTO inventory (
    product_id, 
    warehouse_id, 
    quantity, 
    safety_stock, 
    reorder_level, 
    unit_cost, 
    last_counted_at,
    is_active,
    created_at,
    updated_at
)
SELECT 
    p.id AS product_id,
    w.id AS warehouse_id,
    
    -- Số lượng tồn kho ngẫu nhiên từ 0 đến 100
    FLOOR(RANDOM() * 101)::INT AS quantity, 
    
    -- Mức tồn kho an toàn (cố định hoặc random nhỏ)
    10 AS safety_stock,
    
    -- Mức đặt hàng lại
    20 AS reorder_level,
    
    -- Giá vốn (Unit Cost) giả định bằng 60-80% giá bán (price) của sản phẩm
    -- Nếu sản phẩm chưa có giá thì để mặc định 0
    COALESCE(p.price, 0) * (0.6 + RANDOM() * 0.2) AS unit_cost,
    
    NOW() AS last_counted_at,
    TRUE AS is_active,
    NOW() AS created_at,
    NOW() AS updated_at

FROM product p
CROSS JOIN warehouse w

-- Chỉ thêm những cặp chưa tồn tại để tránh lỗi Duplicate Key
ON CONFLICT (product_id, warehouse_id) DO NOTHING;

-- Log kết quả ra màn hình (Optional)
DO $$
DECLARE 
    _count INT;
BEGIN
    SELECT COUNT(*) INTO _count FROM inventory;
    RAISE NOTICE '✅ Đã khởi tạo xong dữ liệu tồn kho. Tổng số bản ghi hiện tại: %', _count;
END $$;