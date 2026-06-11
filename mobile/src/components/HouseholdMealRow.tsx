import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import type {
  HouseholdMeal,
  MealReplacementResponse,
  MealReplacementScope,
} from "../types/api";
import { colors } from "../theme/colors";
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
  const [showCook, setShowCook] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const name = stringValue(meal.display_name) ?? stringValue(meal.recipe_id) ?? "Meal";
  const slot = stringValue(meal.slot) ?? "meal";
  const scope = stringValue(meal.meal_scope);
  const portionMultiplier = numberValue(meal.portion_multiplier);
  const recipeId = stringValue(meal.recipe_id);
  const replaceScope = replacementScopeFromMeal(scope);
  const ingredients = getTextList(
    meal.ingredients ??
      meal.ingredient_amounts ??
      meal.ingredient_names ??
      meal.ingredients_list ??
      meal.recipe_ingredients,
  );
  const steps = getTextList(meal.cooking_steps ?? meal.directions ?? meal.steps ?? meal.instructions);
  const estimatedTime = firstNumber(
    meal.effective_time_min_for_scoring,
    meal.effective_time_min,
    meal.total_time_min,
    meal.cooking_time_min,
    meal.time_min,
  );

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

      <View style={styles.actionRow}>
        <SmallActionButton
          active={showDetails}
          label="Details"
          onPress={() => setShowDetails((current) => !current)}
        />
        <SmallActionButton
          active={showCook}
          label="Cook / Steps"
          onPress={() => setShowCook((current) => !current)}
        />
        {recipeId ? (
          <SmallActionButton
            active={showAlternatives}
            label="Alternatives"
            onPress={() => setShowAlternatives((current) => !current)}
          />
        ) : null}
      </View>

      {showDetails ? (
        <View style={styles.detailBox}>
          <Text style={styles.detailTitle}>{name}</Text>
          <MetricLine label="Calories" value={`${formatOptionalNumber(meal.kcal, 0)} kcal`} />
          <MetricLine label="Protein" value={`${formatOptionalNumber(meal.protein_g, 1)}g`} />
          <MetricLine label="Carbs" value={`${formatOptionalNumber(meal.carbs_g, 1)}g`} />
          <MetricLine label="Fat" value={`${formatOptionalNumber(meal.fat_g, 1)}g`} />
          <MetricLine
            label="Cooking time"
            value={estimatedTime !== null ? `${Math.round(estimatedTime)} min` : "-"}
          />
          {ingredients.length ? (
            <View style={styles.inlineList}>
              <Text style={styles.detailTitle}>Ingredients</Text>
              {ingredients.slice(0, 8).map((ingredient, index) => (
                <Text key={`${ingredient}-${index}`} style={styles.detailText}>
                  {ingredient}
                </Text>
              ))}
            </View>
          ) : null}
        </View>
      ) : null}

      {showCook ? (
        <View style={styles.detailBox}>
          <Text style={styles.detailTitle}>{name}</Text>
          <MetricLine
            label="Estimated time"
            value={estimatedTime !== null ? `${Math.round(estimatedTime)} min` : "-"}
          />
          {ingredients.length ? (
            <View style={styles.inlineList}>
              <Text style={styles.detailTitle}>Ingredients</Text>
              {ingredients.map((ingredient, index) => (
                <Text key={`${ingredient}-${index}`} style={styles.detailText}>
                  {ingredient}
                </Text>
              ))}
            </View>
          ) : null}
          <View style={styles.inlineList}>
            <Text style={styles.detailTitle}>Cooking steps</Text>
            {steps.length ? (
              steps.map((step, index) => (
                <Text key={`${step}-${index}`} style={styles.detailText}>
                  {index + 1}. {step}
                </Text>
              ))
            ) : (
              <Text style={styles.detailText}>
                Cooking steps are not available for this recipe yet.
              </Text>
            )}
          </View>
        </View>
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

function SmallActionButton({
  active,
  label,
  onPress,
}: {
  active?: boolean;
  label: string;
  onPress: () => void;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [
        styles.alternativesButton,
        active ? styles.alternativesButtonActive : null,
        pressed ? styles.alternativesButtonPressed : null,
      ]}
    >
      <Text
        style={[
          styles.alternativesButtonText,
          active ? styles.alternativesButtonTextActive : null,
        ]}
      >
        {label}
      </Text>
    </Pressable>
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

function MetricLine({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.metricLine}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text style={styles.metricValueCompact}>{value}</Text>
    </View>
  );
}

function firstNumber(...values: unknown[]): number | null {
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function getTextList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (typeof item === "string") {
          return item.trim();
        }
        if (typeof item === "object" && item !== null && "text" in item) {
          return String((item as { text?: unknown }).text ?? "").trim();
        }
        return String(item ?? "").trim();
      })
      .filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(/\n|;/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
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
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  alternativesButtonActive: {
    backgroundColor: colors.accent,
  },
  alternativesButtonPressed: {
    opacity: 0.82,
  },
  alternativesButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "800",
  },
  alternativesButtonTextActive: {
    color: "#FFFFFF",
  },
  actionRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  container: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 10,
    padding: 12,
  },
  detailBox: {
    backgroundColor: colors.background,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 8,
    padding: 12,
  },
  detailText: {
    color: colors.muted,
    fontSize: 13,
    lineHeight: 18,
  },
  detailTitle: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
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
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "700",
  },
  metricValue: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
  },
  metricLine: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  metricValueCompact: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 13,
    fontWeight: "800",
    textAlign: "right",
  },
  inlineList: {
    gap: 4,
  },
  name: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
  },
  nameBlock: {
    flex: 1,
    gap: 2,
  },
  scope: {
    color: colors.accent,
    flexShrink: 0,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  slot: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
});
