from .base import BaseGenerator
from logger import logger

class InventoryLogGenerator(BaseGenerator):
    def __init__(self):
        super().__init__()
        self.sql_command = """
            DO $$
            DECLARE
                _inventory_id BIGINT;
                _current_qty INT;
                _change_type VARCHAR(50);
                _quantity_change INT;
                _quantity_after INT;
                _reference_type VARCHAR(50);
                _note TEXT;
                _changed_by VARCHAR(100);
                _roll FLOAT;
            BEGIN
                -- ==================================================================
                -- BƯỚC 1: CHỌN MỘT BẢN GHI INVENTORY NGẪU NHIÊN
                -- ==================================================================
                SELECT id, quantity
                INTO _inventory_id, _current_qty
                FROM inventory
                WHERE is_active = TRUE
                ORDER BY RANDOM() LIMIT 1;

                IF _inventory_id IS NULL THEN
                    RAISE NOTICE 'Không tìm thấy bản ghi inventory nào.';
                    RETURN;
                END IF;

                -- ==================================================================
                -- BƯỚC 2: CHỌN LOẠI THAY ĐỔI VỚI XÁC SUẤT THỰC TẾ
                -- ==================================================================
                _roll := RANDOM();
                IF _roll < 0.40 THEN
                    -- Restock: Nhập hàng vào kho (40%)
                    _change_type := 'restock';
                    _quantity_change := FLOOR(RANDOM() * 50 + 10)::INT;  -- +10 đến +59
                    _reference_type := 'purchase_order';
                    _note := 'Nhập hàng từ nhà cung cấp';
                    _changed_by := 'Warehouse_Staff';
                ELSIF _roll < 0.70 THEN
                    -- Sale: Xuất hàng do bán (30%)
                    _change_type := 'sale';
                    _quantity_change := -1 * FLOOR(RANDOM() * 5 + 1)::INT;  -- -1 đến -5
                    _reference_type := 'order';
                    _note := 'Xuất hàng cho đơn hàng';
                    _changed_by := 'System';
                ELSIF _roll < 0.85 THEN
                    -- Adjustment: Kiểm kê điều chỉnh (15%)
                    _change_type := 'adjustment';
                    _quantity_change := FLOOR(RANDOM() * 11 - 5)::INT;  -- -5 đến +5
                    _reference_type := 'manual';
                    _note := 'Điều chỉnh sau kiểm kê';
                    _changed_by := (ARRAY['Admin', 'Warehouse_Manager', 'Auditor'])[
                        FLOOR(RANDOM() * 3 + 1)::INT];
                ELSIF _roll < 0.95 THEN
                    -- Damage: Hàng hư hỏng (10%)
                    _change_type := 'damage';
                    _quantity_change := -1 * FLOOR(RANDOM() * 3 + 1)::INT;  -- -1 đến -3
                    _reference_type := 'manual';
                    _note := (ARRAY[
                        'Hàng bị ẩm mốc', 'Hàng bị vỡ trong kho',
                        'Hàng hết hạn sử dụng', 'Hàng bị lỗi từ nhà sản xuất'
                    ])[FLOOR(RANDOM() * 4 + 1)::INT];
                    _changed_by := 'QC_Staff';
                ELSE
                    -- Return: Trả hàng từ khách (5%)
                    _change_type := 'return';
                    _quantity_change := FLOOR(RANDOM() * 2 + 1)::INT;  -- +1 đến +2
                    _reference_type := 'return';
                    _note := 'Nhận hàng trả lại từ khách';
                    _changed_by := 'CS_Staff';
                END IF;

                -- ==================================================================
                -- BƯỚC 3: ĐẢM BẢO TỒN KHO KHÔNG ÂM
                -- ==================================================================
                _quantity_after := _current_qty + _quantity_change;
                IF _quantity_after < 0 THEN
                    -- Nếu không đủ hàng, chỉ xuất tối đa số hiện có
                    _quantity_change := -1 * _current_qty;
                    _quantity_after := 0;
                END IF;

                -- Bỏ qua nếu không có thay đổi thực sự
                IF _quantity_change = 0 THEN
                    RAISE NOTICE 'Không có thay đổi tồn kho (quantity_change = 0). Bỏ qua.';
                    RETURN;
                END IF;

                -- ==================================================================
                -- BƯỚC 4: CẬP NHẬT INVENTORY VÀ GHI LOG
                -- ==================================================================
                UPDATE inventory
                SET quantity = _quantity_after,
                    updated_at = NOW()::TIMESTAMP
                WHERE id = _inventory_id;

                INSERT INTO inventory_log (
                    inventory_id, change_type, quantity_change,
                    quantity_before, quantity_after,
                    reference_type, note, changed_by, created_at
                ) VALUES (
                    _inventory_id, _change_type, _quantity_change,
                    _current_qty, _quantity_after,
                    _reference_type, _note, _changed_by, NOW()::TIMESTAMP
                );

                RAISE NOTICE 'Inventory Log: % | ID: % | Trước: % | Thay đổi: % | Sau: %',
                    _change_type, _inventory_id, _current_qty, _quantity_change, _quantity_after;
            END $$;
        """

    def generate(self, params=None):
        result = False
        try:
            result = super().generate()
            if result:
                logger.info("Generated Inventory Log thành công.")
            return result
        except Exception as e:
            logger.error(f"Lỗi trong InventoryLogGenerator: {e}")
            return False
