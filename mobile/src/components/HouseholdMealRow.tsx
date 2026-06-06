import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import type {
  HouseholdMeal,
  MealReplacementResponse,
  MealReplacementScope,
} from "../types/api";
import { RecipeAlternativesPanel } from "./RecipeAlternativesPanel";

type HouseholdMealRowProps = {
  meal: HouseholdMeal;
  dayIndex?: number;
  datasetProfile?: string;
  householdId?: string;
  memberId?: string;
  planId?: string;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

export function HouseholdMealRow({
  meal,
  dayIndex = 1,
  datasetProfile,
  householdId,
  memberId,
  planId,
  onReplacementApplied,
}: HouseholdMealRowProps) {
  const [showAlternatives, setShowAlternatives] = useState(false);
  const name = stringValue(meal.display_name) ?? stringValue(meal.recipe_id) ?? "Meal";
  const slot = stringValue(meal.slot) ?? "meal";
  const scope = stringValue(meal.meal_scope);
  const portionMultiplier = numberValue(meal.portion_multiplier);
  const recipeId = stringValue(meal.recipe_id);
  const replaceScope = replacementScopeFromMeal(scope);

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

      {recipeId ? (
        <Pressable
          accessibilityRole="button"
          onPress={() => setShowAlternatives((current) => !current)}
          style={({ pressed }) => [
            styles.alternativesButton,
            pressed ? styles.alternativesButtonPressed : null,
          ]}
        >
          <Text style={styles.alternativesButtonText}>
            {showAlternatives ? "Hide alternatives" : "Alternatives"}
          </Text>
        </Pressable>
      ) : null}

      {recipeId ? (
        <RecipeAlternativesPanel
          dayIndex={dayIndex}
          datasetProfile={datasetProfile}
          generationType="household"
          householdId={householdId}
          isVisible={showAlternatives}
          memberId={memberId}
          memberProfileId={memberId}
          onReplacementApplied={onReplacementApplied}
          planId={planId}
          replaceScope={replaceScope}
          slot={slot}
          sourceRecipeId={recipeId}
        />
      ) : null}
    </View>
  );
}

function replacementScopeFromMeal(scope: string | null): MealReplacementScope {
  return scope === "shared" ? "household_shared_meal" : "household_member_meal";
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
  alternativesButton: {
    alignSelf: "flex-start",
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  alternativesButtonPressed: {
    opacity: 0.82,
  },
  alternativesButtonText: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
  },
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
