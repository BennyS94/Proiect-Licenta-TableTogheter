import { useEffect, useMemo, useState } from "react";
import { Alert, Pressable, StyleSheet, Text, View } from "react-native";

import type { GroceryListItem, GroceryListResponse } from "../types/api";
import { colors } from "../theme/colors";
import { GroceryItemRow } from "./GroceryItemRow";

type GroceryListSectionProps = {
  emptyMessage?: string;
  groceryList: GroceryListResponse | null;
  title?: string;
};

type GroceryGroup = {
  category: string;
  categoryKey: string;
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
  pantry_basics: "Pantry Basics / Check At Home",
  sauces_canned: "Sauces & canned",
  seasonings_spices: "Seasonings & spices",
  sweeteners: "Sweeteners",
  vegetables: "Vegetables",
};

export function GroceryListSection({
  emptyMessage = "No grocery list returned for this plan.",
  groceryList,
  title = "Grocery list",
}: GroceryListSectionProps) {
  if (!groceryList) {
    return (
      <View style={styles.panel}>
        <Text style={styles.title}>{title}</Text>
        <Text style={styles.mutedText}>{emptyMessage}</Text>
      </View>
    );
  }

  return <GroceryListContent groceryList={groceryList} title={title} />;
}

function GroceryListContent({
  groceryList,
  title,
}: {
  groceryList: GroceryListResponse;
  title: string;
}) {
  const rawItems = useMemo(() => groceryItems(groceryList), [groceryList]);
  const items = useMemo(() => aggregateDuplicateItems(rawItems), [rawItems]);
  const groups = useMemo(() => groupItems(items), [items]);
  const itemKeys = useMemo(() => items.map((item, index) => itemKey(item, index)), [items]);
  const groupKeys = useMemo(() => groups.map((group) => group.categoryKey), [groups]);
  const [checkedItems, setCheckedItems] = useState<Record<string, boolean>>({});
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});
  const [pantryEnabled, setPantryEnabled] = useState(true);
  const summary = asRecord(groceryList.summary);
  const currency = summaryCurrency(groceryList, summary);
  const selectedItems = items.filter((item, index) => {
    const category = categoryLabel(item);
    const checked = checkedItems[itemKey(item, index)] ?? true;
    return checked && (pantryEnabled || !isPantryCategory(category));
  });
  const selectedPricedItems = selectedItems.filter((item) => itemEstimatedCost(item) !== null);
  const selectedMissingPriceCount = selectedItems.length - selectedPricedItems.length;
  const selectedEstimatedTotal = selectedPricedItems.reduce(
    (total, item) => total + (itemEstimatedCost(item) ?? 0),
    0,
  );
  const estimatedTotalText =
    selectedPricedItems.length
      ? `${selectedEstimatedTotal.toFixed(2)} ${currency}`
      : "Not available";

  useEffect(() => {
    setCheckedItems((current) => {
      const next: Record<string, boolean> = {};
      for (const key of itemKeys) {
        next[key] = current[key] ?? true;
      }
      return next;
    });
  }, [itemKeys]);

  useEffect(() => {
    setExpandedGroups((current) => {
      const next: Record<string, boolean> = {};
      for (const key of groupKeys) {
        next[key] = current[key] ?? true;
      }
      return next;
    });
  }, [groupKeys]);

  function toggleItem(item: GroceryListItem, index: number) {
    const key = itemKey(item, index);
    setCheckedItems((current) => ({
      ...current,
      [key]: !(current[key] ?? true),
    }));
  }

  function toggleGroup(categoryKey: string) {
    setExpandedGroups((current) => ({
      ...current,
      [categoryKey]: !(current[categoryKey] ?? true),
    }));
  }

  return (
    <View style={styles.panel}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>{title}</Text>
        <Text style={styles.meta}>
          {selectedItems.length} / {items.length} items
        </Text>
      </View>

      <View style={styles.summaryCard}>
        <View style={styles.summaryVisual}>
          <View style={styles.summaryVisualPlate} />
          <View style={styles.summaryVisualLeafOne} />
          <View style={styles.summaryVisualLeafTwo} />
          <View style={styles.summaryVisualDot} />
        </View>
        <View style={styles.summaryCopy}>
          <Text style={styles.summaryLabel}>Estimated total</Text>
          <Text numberOfLines={1} adjustsFontSizeToFit style={styles.summaryValue}>
            {estimatedTotalText}
          </Text>
          <Text style={styles.summaryMeta}>
            {selectedItems.length} / {items.length} selected
          </Text>
          <Text style={styles.summaryMeta}>
            Missing prices {selectedMissingPriceCount} items
          </Text>
        </View>
      </View>

      <View style={styles.actionRow}>
        <Pressable
          accessibilityRole="button"
          onPress={() => Alert.alert("Sharing coming soon")}
          style={({ pressed }) => [
            styles.actionButton,
            styles.actionButtonPrimary,
            pressed ? styles.pressed : null,
          ]}
        >
          <Text style={styles.actionButtonPrimaryText}>Send to</Text>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          onPress={() => Alert.alert("Copy coming soon")}
          style={({ pressed }) => [
            styles.actionButton,
            styles.actionButtonSecondary,
            pressed ? styles.pressed : null,
          ]}
        >
          <Text style={styles.actionButtonSecondaryText}>Copy</Text>
        </Pressable>
      </View>

      {groups.length ? (
        <View style={styles.groups}>
          {groups.map((group) => (
            <View key={group.category} style={styles.group}>
              <View style={styles.categoryHeader}>
                <Pressable
                  accessibilityRole="button"
                  onPress={() => toggleGroup(group.categoryKey)}
                  style={({ pressed }) => [
                    styles.categoryToggle,
                    pressed ? styles.pressed : null,
                  ]}
                >
                  <View style={styles.categoryTextBlock}>
                    <Text style={styles.category}>{group.category}</Text>
                    <Text style={styles.categoryMeta}>{group.items.length} items</Text>
                  </View>
                  <Text style={styles.chevron}>
                    {expandedGroups[group.categoryKey] ?? true ? "^" : "v"}
                  </Text>
                </Pressable>
                {isPantryCategory(group.category) ? (
                  <Pressable
                    accessibilityRole="switch"
                    accessibilityState={{ checked: pantryEnabled }}
                    onPress={() => setPantryEnabled((current) => !current)}
                    style={[
                      styles.pantrySwitch,
                      pantryEnabled ? styles.pantrySwitchOn : styles.pantrySwitchOff,
                    ]}
                  >
                    <View
                      style={[
                        styles.pantrySwitchKnob,
                        pantryEnabled ? styles.pantrySwitchKnobOn : null,
                      ]}
                    />
                  </Pressable>
                ) : null}
              </View>
              {isPantryCategory(group.category) && !pantryEnabled ? (
                <Text style={styles.pantryNote}>Assumed available at home</Text>
              ) : null}
              {expandedGroups[group.categoryKey] ?? true
                ? group.items.map((item, index) => {
                    const key = itemKey(item, index);
                    const pantryDisabled =
                      isPantryCategory(group.category) && !pantryEnabled;
                    return (
                      <GroceryItemRow
                        checked={(checkedItems[key] ?? true) && !pantryDisabled}
                        disabled={pantryDisabled}
                        item={item}
                        key={key}
                        onToggle={() => toggleItem(item, index)}
                      />
                    );
                  })
                : null}
            </View>
          ))}
        </View>
      ) : (
        <Text style={styles.mutedText}>No grocery items returned.</Text>
      )}
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

function aggregateDuplicateItems(items: GroceryListItem[]): GroceryListItem[] {
  const grouped = new Map<string, GroceryListItem & { __duplicate_count?: number }>();
  const order: string[] = [];

  for (const item of items) {
    // Grupam conservator doar in aceeasi categorie, ca sa nu unim iteme diferite semantic.
    const category = categoryLabel(item);
    const key = duplicateGroupKey(item, category);
    const existing = grouped.get(key);
    if (!existing) {
      grouped.set(key, {
        ...item,
        __duplicate_count: 1,
        __ui_key: key,
      });
      order.push(key);
      continue;
    }

    existing.__duplicate_count = (existing.__duplicate_count ?? 1) + 1;
    const neededGrams = sumNullableNumbers(neededGramsValue(existing), neededGramsValue(item));
    const estimatedCost = sumNullableNumbers(
      itemEstimatedCost(existing),
      itemEstimatedCost(item),
    );

    if (neededGrams !== null) {
      existing.needed_grams = neededGrams;
      existing.needed_grams_exact = neededGrams;
      existing.total_grams = neededGrams;
      existing.needed_grams_display = formatAmountGrams(neededGrams);
      existing.display_grams = formatAmountGrams(neededGrams);
      existing.purchase_display = aggregatePurchaseDisplay(existing, neededGrams);
    }
    if (estimatedCost !== null) {
      existing.estimated_cost = estimatedCost;
      existing.estimated_cost_display = undefined;
    }
  }

  return order.map((key) => {
    const item = grouped.get(key) as GroceryListItem & { __duplicate_count?: number };
    if ((item.__duplicate_count ?? 1) > 1) {
      item.grocery_item_id = key;
    }
    return item;
  });
}

function groupItems(items: GroceryListItem[]): GroceryGroup[] {
  const groups: GroceryGroup[] = [];
  for (const item of items) {
    const category = categoryLabel(item);
    const categoryKey = normalizeKey(category);
    const existingGroup = groups.find((group) => group.category === category);
    if (existingGroup) {
      existingGroup.items.push(item);
    } else {
      groups.push({ category, categoryKey, items: [item] });
    }
  }
  return groups;
}

function duplicateGroupKey(item: GroceryListItem, category: string): string {
  const name =
    stringValue(item.canonical_name) ??
    stringValue(item.display_name_clean) ??
    stringValue(item.display_name) ??
    "grocery_item";
  return `${normalizeKey(category)}:${normalizeKey(name)}`;
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

function isPantryCategory(category: string): boolean {
  return normalizeKey(category) === normalizeKey(CATEGORY_LABELS.pantry_basics);
}

function neededGramsValue(item: GroceryListItem): number | null {
  return numberValue(item.needed_grams_exact ?? item.needed_grams ?? item.total_grams);
}

function itemEstimatedCost(item: GroceryListItem): number | null {
  return numberValue(item.estimated_cost);
}

function sumNullableNumbers(left: number | null, right: number | null): number | null {
  if (left === null && right === null) {
    return null;
  }
  return roundNumber((left ?? 0) + (right ?? 0), 2);
}

function aggregatePurchaseDisplay(item: GroceryListItem, neededGrams: number): string {
  const original = stringValue(item.purchase_display) ?? "";
  const amount = formatAmountGrams(roundPurchaseAmount(neededGrams));
  if (original.toLowerCase().includes("check pantry")) {
    return `check pantry; need about ${amount}`;
  }
  if (original.toLowerCase().includes("review item")) {
    return `review item; need about ${amount}`;
  }
  return `about ${amount}`;
}

function roundPurchaseAmount(grams: number): number {
  if (grams >= 1000) {
    return Math.ceil(grams / 100) * 100;
  }
  if (grams >= 100) {
    return Math.ceil(grams / 50) * 50;
  }
  return Math.ceil(grams / 10) * 10;
}

function formatAmountGrams(grams: number): string {
  if (grams >= 1000) {
    return `~${formatCompactNumber(grams / 1000)}kg`;
  }
  return `~${Math.round(grams)}g`;
}

function formatCompactNumber(value: number): string {
  return value.toLocaleString("en-US", {
    maximumFractionDigits: value >= 10 ? 0 : 2,
    minimumFractionDigits: 0,
  });
}

function roundNumber(value: number, digits: number): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function normalizeKey(value: string): string {
  return value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
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

function itemKey(item: GroceryListItem, index: number): string {
  return String(
    item.__ui_key ??
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
  actionButton: {
    alignItems: "center",
    borderRadius: 16,
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 16,
  },
  actionButtonPrimary: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
    borderWidth: 1,
  },
  actionButtonPrimaryText: {
    color: colors.card,
    fontSize: 15,
    fontWeight: "900",
  },
  actionButtonSecondary: {
    backgroundColor: "#F8FBF3",
    borderColor: "#CFE3BF",
    borderWidth: 1,
  },
  actionButtonSecondaryText: {
    color: colors.accentDark,
    fontSize: 15,
    fontWeight: "900",
  },
  actionRow: {
    flexDirection: "row",
    gap: 10,
  },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  pressed: {
    opacity: 0.82,
  },
  title: {
    color: colors.text,
    fontSize: 18,
    fontWeight: "800",
  },
  meta: {
    color: colors.mutedSoft,
    fontSize: 14,
    fontWeight: "700",
    textAlign: "right",
  },
  summaryCard: {
    alignItems: "center",
    backgroundColor: "#F1F8E9",
    borderColor: "#DDEAD3",
    borderRadius: 22,
    borderWidth: 1,
    elevation: 1,
    flexDirection: "row",
    gap: 14,
    minHeight: 112,
    padding: 14,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 5 },
    shadowOpacity: 0.05,
    shadowRadius: 10,
  },
  summaryCopy: {
    flex: 1,
    minWidth: 0,
  },
  summaryLabel: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  summaryMeta: {
    color: colors.mutedSoft,
    fontSize: 13,
    fontWeight: "700",
    marginTop: 3,
  },
  summaryValue: {
    color: "#1B2430",
    fontSize: 27,
    fontWeight: "900",
    lineHeight: 34,
    marginTop: 4,
  },
  summaryVisual: {
    alignItems: "center",
    backgroundColor: "#F7CF53",
    borderRadius: 20,
    height: 82,
    justifyContent: "center",
    overflow: "hidden",
    width: 82,
  },
  summaryVisualDot: {
    backgroundColor: "#F8B84E",
    borderRadius: 8,
    height: 16,
    position: "absolute",
    right: 20,
    top: 31,
    width: 16,
  },
  summaryVisualLeafOne: {
    backgroundColor: colors.accent,
    borderBottomLeftRadius: 12,
    borderTopRightRadius: 12,
    height: 22,
    position: "absolute",
    right: 17,
    top: 22,
    transform: [{ rotate: "-18deg" }],
    width: 32,
  },
  summaryVisualLeafTwo: {
    backgroundColor: "#A8D66D",
    borderBottomLeftRadius: 11,
    borderTopRightRadius: 11,
    bottom: 19,
    height: 19,
    left: 19,
    position: "absolute",
    transform: [{ rotate: "18deg" }],
    width: 29,
  },
  summaryVisualPlate: {
    backgroundColor: "rgba(255,255,255,0.92)",
    borderRadius: 28,
    height: 48,
    transform: [{ rotate: "-10deg" }],
    width: 58,
  },
  groups: {
    gap: 16,
  },
  group: {
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 8,
    backgroundColor: colors.card,
    padding: 14,
    gap: 12,
  },
  category: {
    color: colors.accent,
    fontSize: 15,
    fontWeight: "800",
  },
  categoryHeader: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  categoryMeta: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "700",
  },
  categoryTextBlock: {
    flex: 1,
    gap: 2,
    minWidth: 0,
  },
  categoryToggle: {
    alignItems: "center",
    flex: 1,
    flexDirection: "row",
    gap: 10,
    minHeight: 34,
  },
  chevron: {
    color: colors.accentDark,
    fontSize: 16,
    fontWeight: "900",
    width: 18,
  },
  pantryNote: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "700",
    marginTop: -4,
  },
  pantrySwitch: {
    borderRadius: 12,
    height: 24,
    justifyContent: "center",
    paddingHorizontal: 3,
    width: 44,
  },
  pantrySwitchKnob: {
    backgroundColor: "#FFFFFF",
    borderRadius: 9,
    height: 18,
    width: 18,
  },
  pantrySwitchKnobOn: {
    alignSelf: "flex-end",
  },
  pantrySwitchOff: {
    backgroundColor: "#D1D5DB",
  },
  pantrySwitchOn: {
    backgroundColor: colors.accent,
  },
  mutedText: {
    color: colors.mutedSoft,
    fontSize: 15,
  },
});
