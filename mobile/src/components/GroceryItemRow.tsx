import { StyleSheet, Text, View } from "react-native";

import type { GroceryListItem } from "../types/api";

type GroceryItemRowProps = {
  item: GroceryListItem;
};

export function GroceryItemRow({ item }: GroceryItemRowProps) {
  const warnings = itemWarnings(item);
  const priceText = formatCost(item);
  const missingPrice = !priceText;

  return (
    <View style={styles.row}>
      <View style={styles.main}>
        <Text style={styles.name}>{displayName(item)}</Text>
        <Text style={styles.detail}>Need: {neededAmount(item)}</Text>
        <Text style={styles.detail}>Buy: {purchaseSuggestion(item)}</Text>
        {warnings.slice(0, 2).map((warning, index) => (
          <Text key={`${warning}-${index}`} style={styles.warningText}>
            {warning}
          </Text>
        ))}
      </View>
      <View style={styles.side}>
        <Text style={missingPrice ? styles.missingPrice : styles.cost}>
          {priceText ?? "Price unavailable"}
        </Text>
        {missingPrice || hasPriceMissingWarning(item) ? (
          <Text style={styles.badge}>Price missing</Text>
        ) : null}
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

function itemWarnings(item: GroceryListItem): string[] {
  return [
    ...normaliseWarnings(item.warnings),
    ...normaliseWarnings(item.purchase_warnings),
    ...normaliseWarnings(item.price_warning),
  ].filter((value, index, values) => value.length > 0 && values.indexOf(value) === index);
}

function hasPriceMissingWarning(item: GroceryListItem): boolean {
  return itemWarnings(item).some((warning) =>
    warning.toLowerCase().includes("price_missing"),
  );
}

function normaliseWarnings(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  if (value == null) {
    return [];
  }
  return [String(value)];
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
    borderTopColor: "#E5E0D5",
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
    paddingTop: 12,
  },
  main: {
    flex: 1,
    gap: 3,
  },
  name: {
    color: "#1F2933",
    fontSize: 15,
    fontWeight: "800",
  },
  detail: {
    color: "#4B5563",
    fontSize: 13,
    fontWeight: "600",
  },
  side: {
    alignItems: "flex-end",
    minWidth: 104,
  },
  cost: {
    color: "#147A4A",
    fontSize: 13,
    fontWeight: "800",
    textAlign: "right",
  },
  missingPrice: {
    color: "#7A4B00",
    fontSize: 12,
    fontWeight: "800",
    textAlign: "right",
  },
  badge: {
    color: "#7A4B00",
    fontSize: 11,
    fontWeight: "800",
    marginTop: 4,
    textAlign: "right",
  },
  warningText: {
    color: "#7A4B00",
    fontSize: 12,
    fontWeight: "700",
  },
});
