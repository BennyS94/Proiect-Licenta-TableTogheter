import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import type { FeedbackType, GeneratedMeal, MealReplacementResponse } from "../types/api";
import { colors } from "../theme/colors";
import { MealFeedbackButtons } from "./MealFeedbackButtons";
import { RecipeAlternativesPanel } from "./RecipeAlternativesPanel";

type MealRowProps = {
  meal: GeneratedMeal;
  dayIndex?: number;
  datasetProfile?: string;
  householdId?: string;
  memberProfileId?: string;
  memberProfile?: Record<string, unknown>;
  planId?: string;
  feedbackDisabled?: boolean;
  pendingFeedbackType?: FeedbackType | null;
  onSubmitFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => void;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

export function MealRow({
  meal,
  dayIndex = 1,
  datasetProfile,
  householdId,
  memberProfileId,
  memberProfile,
  planId,
  feedbackDisabled,
  pendingFeedbackType,
  onSubmitFeedback,
  onReplacementApplied,
}: MealRowProps) {
  const [showAlternatives, setShowAlternatives] = useState(false);
  const [showCook, setShowCook] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const recipeId = stringValue(meal.recipe_id);
  const slot = stringValue(meal.slot);
  const ingredients = getIngredients(meal);
  const steps = getCookingSteps(meal);
  const estimatedTime = getEstimatedTime(meal);

  return (
    <View style={styles.container}>
      <View style={styles.row}>
        <View style={styles.main}>
          <Text style={styles.slot}>{String(meal.slot ?? "meal")}</Text>
          <Text style={styles.name}>{String(meal.display_name ?? meal.recipe_id ?? "Recipe")}</Text>
        </View>
        <View style={styles.macros}>
          <Text style={styles.macro}>{formatNumber(meal.kcal)} kcal</Text>
          <Text style={styles.macro}>{formatNumber(meal.protein_g)}g protein</Text>
        </View>
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
          <Text style={styles.detailTitle}>{String(meal.display_name ?? meal.recipe_id ?? "Recipe")}</Text>
          <MetricLine label="Calories" value={`${formatNumber(meal.kcal)} kcal`} />
          <MetricLine label="Protein" value={`${formatNumber(meal.protein_g)}g`} />
          <MetricLine label="Carbs" value={`${formatNumber(meal.carbs_g)}g`} />
          <MetricLine label="Fats" value={`${formatNumber(meal.fat_g)}g`} />
          <MetricLine label="Cooking time" value={estimatedTime ? `${estimatedTime} min` : "-"} />
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
          <Text style={styles.detailTitle}>{String(meal.display_name ?? meal.recipe_id ?? "Recipe")}</Text>
          <MetricLine label="Estimated time" value={estimatedTime ? `${estimatedTime} min` : "-"} />
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
      {onSubmitFeedback ? (
        <MealFeedbackButtons
          disabled={feedbackDisabled}
          meal={meal}
          onSubmit={onSubmitFeedback}
          pendingFeedbackType={pendingFeedbackType}
        />
      ) : null}
      {recipeId ? (
        <RecipeAlternativesPanel
          dayIndex={dayIndex}
          datasetProfile={datasetProfile}
          generationType="individual"
          householdId={householdId}
          isVisible={showAlternatives}
          memberProfile={memberProfile}
          memberProfileId={memberProfileId}
          onReplacementApplied={onReplacementApplied}
          planId={planId}
          replaceScope="individual_meal"
          slot={slot ?? undefined}
          sourceRecipeId={recipeId}
        />
      ) : null}
    </View>
  );
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

function MetricLine({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.metricLine}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text style={styles.metricValue}>{value}</Text>
    </View>
  );
}

function formatNumber(value: unknown): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return String(Math.round(value));
}

function getEstimatedTime(meal: GeneratedMeal): number | null {
  const candidates = [
    meal.effective_time_min_for_scoring,
    meal.effective_time_min,
    meal.total_time_min,
    meal.cooking_time_min,
    meal.time_min,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === "number" && Number.isFinite(candidate)) {
      return Math.round(candidate);
    }
  }
  return null;
}

function getIngredients(meal: GeneratedMeal): string[] {
  const raw =
    meal.ingredients ??
    meal.ingredient_amounts ??
    meal.ingredient_names ??
    meal.ingredients_list ??
    meal.recipe_ingredients;
  return normalizeTextList(raw);
}

function getCookingSteps(meal: GeneratedMeal): string[] {
  const raw = meal.cooking_steps ?? meal.directions ?? meal.steps ?? meal.instructions;
  return normalizeTextList(raw);
}

function normalizeTextList(value: unknown): string[] {
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

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
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
    borderTopWidth: 1,
    borderTopColor: colors.border,
    gap: 8,
    paddingTop: 10,
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
  inlineList: {
    gap: 4,
  },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  main: {
    flex: 1,
    gap: 2,
  },
  slot: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "800",
    textTransform: "capitalize",
  },
  name: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "700",
  },
  macros: {
    alignItems: "flex-end",
    minWidth: 92,
  },
  macro: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
  },
  metricLabel: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
  },
  metricLine: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  metricValue: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 13,
    fontWeight: "800",
    textAlign: "right",
  },
});
