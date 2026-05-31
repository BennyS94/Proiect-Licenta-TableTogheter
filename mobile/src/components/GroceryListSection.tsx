import { StyleSheet, Text, View } from "react-native";

import type { GroceryListItem, GroceryListResponse } from "../types/api";
import { GroceryItemRow } from "./GroceryItemRow";

type GroceryListSectionProps = {
  groceryList: GroceryListResponse | null;
};

type GroceryGroup = {
  category: string;
  items: GroceryListItem[];
};

const CATEGORY_LABELS: Record<string, string> = {
  carbs_grains: "Carbs & grains",
  dairy_eggs: "Dairy & eggs",
  fruits: "Fruits",
  legumes_beans: "Legumes & beans",
  meat_fish: "Meat & fish",
  oils_fats: "Oils & fats",
  other_review: "Other / review",
  pantry_basics: "Pantry basics",
  sauces_canned: "Sauces & canned",
  seasonings_spices: "Seasonings & spices",
  sweeteners: "Sweeteners",
  vegetables: "Vegetables",
};

export function GroceryListSection({ groceryList }: GroceryListSectionProps) {
  if (!groceryList) {
    return (
      <View style={styles.panel}>
        <Text style={styles.title}>Grocery list</Text>
        <Text style={styles.mutedText}>No grocery list returned for this plan.</Text>
      </View>
    );
  }

  const items = groceryItems(groceryList);
  const groups = groupItems(items);
  const summary = asRecord(groceryList.summary);
  const estimatedTotal = estimatedTotalCost(groceryList, summary);
  const missingPrices = missingPriceCount(groceryList, summary);
  const currency = summaryCurrency(groceryList, summary);
  const warnings = groceryWarnings(groceryList, summary);

  return (
    <View style={styles.panel}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>Grocery list</Text>
        <Text style={styles.meta}>{items.length} items</Text>
      </View>

      <View style={styles.summaryBox}>
        {estimatedTotal !== null ? (
          <InfoLine label="Estimated total" value={`${estimatedTotal.toFixed(2)} ${currency}`} />
        ) : null}
        {missingPrices !== null ? (
          <InfoLine label="Missing prices" value={`${missingPrices} items`} />
        ) : null}
        {estimatedTotal === null && missingPrices === null ? (
          <Text style={styles.mutedText}>No grocery summary returned.</Text>
        ) : null}
      </View>

      {warnings.length ? (
        <View style={styles.warningBox}>
          <Text style={styles.warningTitle}>Warnings</Text>
          {warnings.slice(0, 5).map((warning, index) => (
            <Text key={`${warning}-${index}`} style={styles.warningText}>
              {warning}
            </Text>
          ))}
        </View>
      ) : null}

      {groups.length ? (
        <View style={styles.groups}>
          {groups.map((group) => (
            <View key={group.category} style={styles.group}>
              <Text style={styles.category}>{group.category}</Text>
              {group.items.map((item, index) => (
                <GroceryItemRow key={itemKey(item, index)} item={item} />
              ))}
            </View>
          ))}
        </View>
      ) : (
        <Text style={styles.mutedText}>No grocery items returned.</Text>
      )}
    </View>
  );
}

function InfoLine({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.infoLine}>
      <Text style={styles.infoLabel}>{label}</Text>
      <Text style={styles.infoValue}>{value}</Text>
    </View>
  );
}

function groceryItems(groceryList: GroceryListResponse): GroceryListItem[] {
  const displayItems = Array.isArray(groceryList.display_items)
    ? groceryList.display_items
    : [];
  const items = displayItems.length
    ? displayItems
    : Array.isArray(groceryList.items)
      ? groceryList.items
      : [];
  return items.filter(isRecord).map((item) => item as GroceryListItem);
}

function groupItems(items: GroceryListItem[]): GroceryGroup[] {
  const groups: GroceryGroup[] = [];
  for (const item of items) {
    const category = categoryLabel(item);
    const existingGroup = groups.find((group) => group.category === category);
    if (existingGroup) {
      existingGroup.items.push(item);
    } else {
      groups.push({ category, items: [item] });
    }
  }
  return groups;
}

function categoryLabel(item: GroceryListItem): string {
  const rawLabel =
    stringValue(item.category_label) ??
    stringValue(item.category) ??
    stringValue(item.grocery_category) ??
    "other_review";
  const normalized = rawLabel.trim();
  return CATEGORY_LABELS[normalized] ?? titleize(normalized);
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function estimatedTotalCost(
  groceryList: GroceryListResponse,
  summary: Record<string, unknown>,
): number | null {
  const pricing = asRecord(summary.pricing_summary);
  return (
    numberValue(groceryList.total_estimated_cost) ??
    numberValue(groceryList.estimated_total_cost) ??
    numberValue(summary.total_estimated_cost) ??
    numberValue(summary.estimated_total_cost) ??
    numberValue(pricing.total_estimated_cost)
  );
}

function missingPriceCount(
  groceryList: GroceryListResponse,
  summary: Record<string, unknown>,
): number | null {
  const pricing = asRecord(summary.pricing_summary);
  const priceWarningCounts = asRecord(summary.price_warning_counts);
  const pricingWarningCounts = asRecord(pricing.price_warning_counts);
  return (
    numberValue(groceryList.missing_price_count) ??
    numberValue(summary.missing_price_count) ??
    numberValue(summary.unpriced_item_count) ??
    numberValue(pricing.unpriced_item_count) ??
    numberValue(priceWarningCounts.price_missing) ??
    numberValue(pricingWarningCounts.price_missing)
  );
}

function summaryCurrency(
  groceryList: GroceryListResponse,
  summary: Record<string, unknown>,
): string {
  const pricing = asRecord(summary.pricing_summary);
  return (
    stringValue(groceryList.currency) ??
    stringValue(summary.estimated_total_currency) ??
    stringValue(summary.currency) ??
    stringValue(pricing.currency) ??
    "RON"
  );
}

function groceryWarnings(
  groceryList: GroceryListResponse,
  summary: Record<string, unknown>,
): string[] {
  return [
    ...normaliseWarnings(groceryList.warnings),
    ...normaliseWarnings(summary.warnings),
  ].filter((value, index, values) => value.length > 0 && values.indexOf(value) === index);
}

function itemKey(item: GroceryListItem, index: number): string {
  return String(
    item.grocery_item_id ??
      item.display_name_clean ??
      item.display_name ??
      item.canonical_name ??
      index,
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
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
  panel: {
    gap: 14,
  },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  title: {
    color: "#111827",
    fontSize: 18,
    fontWeight: "800",
  },
  meta: {
    color: "#6B7280",
    fontSize: 14,
    fontWeight: "700",
    textAlign: "right",
  },
  summaryBox: {
    borderWidth: 1,
    borderColor: "#D9D6CC",
    borderRadius: 8,
    backgroundColor: "#FFFFFF",
    padding: 14,
    gap: 8,
  },
  infoLine: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 16,
  },
  infoLabel: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "600",
  },
  infoValue: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  groups: {
    gap: 16,
  },
  group: {
    borderWidth: 1,
    borderColor: "#D9D6CC",
    borderRadius: 8,
    backgroundColor: "#FFFFFF",
    padding: 14,
    gap: 12,
  },
  category: {
    color: "#165D77",
    fontSize: 15,
    fontWeight: "800",
  },
  mutedText: {
    color: "#6B7280",
    fontSize: 15,
  },
  warningBox: {
    borderWidth: 1,
    borderColor: "#F4C790",
    borderRadius: 8,
    backgroundColor: "#FFF8ED",
    padding: 12,
    gap: 6,
  },
  warningTitle: {
    color: "#7A4B00",
    fontSize: 14,
    fontWeight: "800",
  },
  warningText: {
    color: "#7A4B00",
    fontSize: 13,
    fontWeight: "600",
  },
});
