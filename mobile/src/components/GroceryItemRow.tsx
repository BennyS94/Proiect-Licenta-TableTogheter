import { Pressable, StyleSheet, Text, View } from "react-native";

import type { GroceryListItem } from "../types/api";
import { colors } from "../theme/colors";

type GroceryItemRowProps = {
  checked?: boolean;
  disabled?: boolean;
  item: GroceryListItem;
  onToggle?: () => void;
};

export function GroceryItemRow({
  checked = true,
  disabled = false,
  item,
  onToggle,
}: GroceryItemRowProps) {
  const priceText = formatCost(item);
  const missingPrice = !priceText;
  const muted = disabled || !checked;

  return (
    <View style={[styles.row, muted ? styles.rowMuted : null]}>
      <View style={styles.main}>
        <Text style={styles.name}>{displayName(item)}</Text>
        <Text style={styles.detail}>Need: {neededAmount(item)}</Text>
        <Text style={styles.detail}>Buy: {purchaseSuggestion(item)}</Text>
      </View>
      <View style={styles.side}>
        <Text style={[missingPrice || muted ? styles.missingPrice : styles.cost]}>
          {priceText ?? "Price unavailable"}
        </Text>
        <Pressable
          accessibilityRole="checkbox"
          accessibilityState={{ checked, disabled }}
          disabled={disabled}
          onPress={onToggle}
          style={({ pressed }) => [
            styles.checkbox,
            checked ? styles.checkboxChecked : styles.checkboxUnchecked,
            disabled ? styles.checkboxDisabled : null,
            pressed && !disabled ? styles.checkboxPressed : null,
          ]}
        >
          <Text style={[styles.checkboxMark, checked ? styles.checkboxMarkChecked : null]}>
            {checked ? "✓" : ""}
          </Text>
        </Pressable>
      </View>
    </View>
  );
}

function displayName(item: GroceryListItem): string {
  return stringValue(item.display_name_clean ?? item.display_name) ?? "Grocery item";
}

function neededAmount(item: GroceryListItem): string {
  const display = stringValue(item.needed_grams_display ?? item.display_grams);
  if (display) {
    return display;
  }
  const grams = numberValue(item.needed_grams_exact ?? item.needed_grams ?? item.total_grams);
  if (grams === null) {
    return "-";
  }
  return `${Math.round(grams)}g`;
}

function purchaseSuggestion(item: GroceryListItem): string {
  return stringValue(item.purchase_display) ?? "-";
}

function formatCost(item: GroceryListItem): string | null {
  const existing = stringValue(item.estimated_cost_display);
  if (existing) {
    return existing;
  }
  const cost = numberValue(item.estimated_cost);
  if (cost === null) {
    return null;
  }
  return `${cost.toFixed(2)} ${stringValue(item.currency) ?? "RON"}`;
}

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function numberValue(value: unknown): number | null {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return null;
  }
  return value;
}

const styles = StyleSheet.create({
  row: {
    borderTopWidth: 1,
    borderTopColor: colors.border,
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
    paddingTop: 12,
  },
  rowMuted: {
    opacity: 0.56,
  },
  main: {
    flex: 1,
    gap: 3,
  },
  name: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
  },
  detail: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "600",
  },
  side: {
    alignItems: "flex-end",
    gap: 7,
    minWidth: 104,
  },
  checkbox: {
    alignItems: "center",
    borderRadius: 8,
    borderWidth: 1,
    height: 26,
    justifyContent: "center",
    width: 26,
  },
  checkboxChecked: {
    backgroundColor: "#EEF7E8",
    borderColor: colors.accent,
  },
  checkboxDisabled: {
    backgroundColor: "#F3F4F6",
    borderColor: "#D1D5DB",
  },
  checkboxMark: {
    color: colors.mutedSoft,
    fontSize: 15,
    fontWeight: "900",
    lineHeight: 18,
  },
  checkboxMarkChecked: {
    color: colors.accentDark,
  },
  checkboxPressed: {
    opacity: 0.75,
  },
  checkboxUnchecked: {
    backgroundColor: "#FFFFFF",
    borderColor: colors.border,
  },
  cost: {
    color: colors.success,
    fontSize: 13,
    fontWeight: "800",
    textAlign: "right",
  },
  missingPrice: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "800",
    textAlign: "right",
  },
});
