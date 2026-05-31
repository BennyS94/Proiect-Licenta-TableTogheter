import { StyleSheet, Text, View } from "react-native";

import type { HouseholdMeal } from "../types/api";

type HouseholdMealRowProps = {
  meal: HouseholdMeal;
};

export function HouseholdMealRow({ meal }: HouseholdMealRowProps) {
  const name = stringValue(meal.display_name) ?? stringValue(meal.recipe_id) ?? "Meal";
  const slot = stringValue(meal.slot) ?? "meal";
  const scope = stringValue(meal.meal_scope);
  const portionMultiplier = numberValue(meal.portion_multiplier);

  return (
    <View style={styles.container}>
      <View style={styles.headerRow}>
        <View style={styles.nameBlock}>
          <Text style={styles.slot}>{titleize(slot)}</Text>
          <Text style={styles.name}>{name}</Text>
        </View>
        {scope ? <Text style={styles.scope}>{scope}</Text> : null}
      </View>

      <View style={styles.metricsRow}>
        {portionMultiplier !== null ? (
          <Metric label="Portion" value={`${formatNumber(portionMultiplier, 2)}x`} />
        ) : null}
        <Metric label="Kcal" value={formatOptionalNumber(meal.kcal, 0)} />
        <Metric label="Protein" value={formatOptionalNumber(meal.protein_g, 1)} />
        <Metric label="Carbs" value={formatOptionalNumber(meal.carbs_g, 1)} />
        <Metric label="Fat" value={formatOptionalNumber(meal.fat_g, 1)} />
      </View>
    </View>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.metric}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text style={styles.metricValue}>{value}</Text>
    </View>
  );
}

function formatOptionalNumber(value: unknown, digits: number): string {
  const parsed = numberValue(value);
  return parsed === null ? "-" : formatNumber(parsed, digits);
}

function formatNumber(value: number, digits: number): string {
  return value.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits > 0 ? 1 : 0,
  });
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

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    gap: 10,
    padding: 12,
  },
  headerRow: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  metricsRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  metric: {
    minWidth: 72,
  },
  metricLabel: {
    color: "#6B7280",
    fontSize: 12,
    fontWeight: "700",
  },
  metricValue: {
    color: "#111827",
    fontSize: 14,
    fontWeight: "800",
  },
  name: {
    color: "#111827",
    fontSize: 15,
    fontWeight: "800",
  },
  nameBlock: {
    flex: 1,
    gap: 2,
  },
  scope: {
    color: "#165D77",
    flexShrink: 0,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  slot: {
    color: "#6B7280",
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
});
