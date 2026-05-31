package com.cardstream.simulator.catalog;

import com.cardstream.common.model.Condition;
import com.cardstream.common.model.Finish;

/**
 * A sellable unit: product × printing × condition (TCGplayer's SKU). The simulator's unit of
 * pricing and the grain of every emitted listing/sale. {@code skuId} is synthetic and numeric.
 */
public record Sku(long skuId, long productId, String cardId, Finish finish, Condition condition) {

    /** TCGplayer-style printing label for the finish (the API's {@code subTypeName}). */
    public String subTypeName() {
        return switch (finish) {
            case NORMAL -> "Normal";
            case HOLOFOIL -> "Holofoil";
            case REVERSE_HOLOFOIL -> "Reverse Holofoil";
        };
    }
}
