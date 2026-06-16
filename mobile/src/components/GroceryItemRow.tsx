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
  const buyText = purchaseSuggestion(item);

  return (
    <Pressable
      accessibilityRole="checkbox"
      accessibilityState={{ checked, disabled }}
      disabled={disabled}
      onPress={onToggle}
      style={({ pressed }) => [
        styles.row,
        muted ? styles.rowMuted : null,
        pressed && !disabled ? styles.rowPressed : null,
      ]}
    >
      <View style={styles.content}>
        <View style={styles.main}>
          <Text style={styles.name}>{displayName(item)}</Text>
          <Text style={styles.detail}>Need: {neededAmount(item)}</Text>
          {buyText ? <Text style={styles.detail}>Buy: {buyText}</Text> : null}
        </View>
        <View style={styles.side}>
          <Text style={[missingPrice || muted ? styles.missingPrice : styles.cost]}>
            {priceText ?? "Price unavailable"}
          </Text>
          <View
            style={[
              styles.checkbox,
              checked ? styles.checkboxChecked : styles.checkboxUnchecked,
              disabled ? styles.checkboxDisabled : null,
            ]}
          >
            <Text style={[styles.checkboxMark, checked ? styles.checkboxMarkChecked : null]}>
              {checked ? "✓" : ""}
            </Text>
          </View>
        </View>
      </View>
    </Pressable>
  );
}

function displayName(item: GroceryListItem): string {
  const name = stringValue(item.display_name_clean ?? item.display_name);
  return normalizeDisplayName(name) ?? "Grocery item";
}

function neededAmount(item: GroceryListItem): string {
  const display = stringValue(item.needed_grams_display ?? item.display_grams);
  if (display) {
    return cleanQuantityText(display);
  }
  const grams = numberValue(item.needed_grams_exact ?? item.needed_grams ?? item.total_grams);
  if (grams === null) {
    return "-";
  }
  return `${Math.round(grams)} g`;
}

function purchaseSuggestion(item: GroceryListItem): string | null {
  if (stringValue(item.purchase_unit_type)?.toLowerCase() === "pantry_check") {
    return null;
  }
  const value = stringValue(item.purchase_display);
  if (!value) {
    return null;
  }
  if (value.toLowerCase().includes("check pantry")) {
    return null;
  }
  return cleanQuantityText(value.replace(/(^|[\s;/])about\s+~?/gi, "$1~"), item);
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

function cleanQuantityText(value: string, item?: GroceryListItem): string {
  let cleaned = value
    .replace(/\bmedium\b/gi, "med.")
    .replace(/\s+[\/·]\s+(~?\d)/g, " $1")
    .replace(
      /(\d+(?:\.\d+)?)(kg|g|ml|l)\b/gi,
      (_match: string, amount: string, unit: string) => `${amount} ${normalizeUnit(unit)}`,
    )
    .replace(/\s{2,}/g, " ")
    .trim();
  if (isGreenOnionItem(item)) {
    cleaned = cleaned.replace(/\bmed\.\s+onions\b/gi, "green onions");
  }
  return cleaned;
}

function normalizeDisplayName(value: string | null): string | null {
  if (!value) {
    return null;
  }
  const normalized = normalizeText(value);
  if (
    normalized.includes("milk fat content unknown") &&
    normalized.includes("uht") &&
    (normalized.includes("sterilized") || normalized.includes("sterilised"))
  ) {
    return "UHT milk";
  }
  if (
    normalized.includes("bread french bread baguette") ||
    normalized.includes("french bread baguette")
  ) {
    return "Baguette";
  }
  if (normalized.includes("wheat flour white all purpose enriched unbleached")) {
    return "All-purpose flour";
  }
  if (normalized.includes("corn tortilla wrap to be filled")) {
    return "Corn tortillas";
  }
  return value;
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
}

function isGreenOnionItem(item: GroceryListItem | undefined): boolean {
  if (!item) {
    return false;
  }
  const text = normalizeText(
    [
      item.display_name_clean,
      item.display_name,
      item.canonical_name,
    ]
      .map((value) => stringValue(value))
      .filter(Boolean)
      .join(" "),
  );
  return text.includes("green onion");
}

function normalizeUnit(value: string): string {
  return value.toLowerCase() === "l" ? "L" : value.toLowerCase();
}

const styles = StyleSheet.create({
  row: {
    borderTopWidth: 1,
    borderTopColor: colors.border,
    paddingTop: 12,
  },
  content: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  rowMuted: {
    opacity: 0.56,
  },
  rowPressed: {
    opacity: 0.78,
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
    fontSize: 12,
    fontWeight: "600",
  },
  side: {
    alignItems: "flex-end",
    gap: 6,
    justifyContent: "center",
    minWidth: 104,
  },
  checkbox: {
    alignItems: "center",
    borderRadius: 7,
    borderWidth: 1,
    height: 24,
    justifyContent: "center",
    width: 24,
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
    fontSize: 14,
    fontWeight: "900",
    lineHeight: 16,
  },
  checkboxMarkChecked: {
    color: colors.accentDark,
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
