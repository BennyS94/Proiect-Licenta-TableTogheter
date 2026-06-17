import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import type { FeedbackType, GeneratedMeal, MealReplacementResponse } from "../types/api";
import { colors } from "../theme/colors";
import { IngredientMeasurementRow } from "./IngredientMeasurementRow";
import { MealFeedbackButtons } from "./MealFeedbackButtons";
import { RecipeAlternativesPanel } from "./RecipeAlternativesPanel";
import { BottomSheet } from "./ui/BottomSheet";
import { MacroMiniStat } from "./ui/MacroMiniStat";
import { formatRecipeDisplayName } from "../utils/formatRecipeDisplayName";

type MealSheet = "cook" | "alternatives";

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
  onSubmitFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onUndoFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => Promise<void> | void;
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
  onUndoFeedback,
  onReplacementApplied,
}: MealRowProps) {
  const [activeSheet, setActiveSheet] = useState<MealSheet | null>(null);
  const recipeId = stringValue(meal.recipe_id);
  const slot = stringValue(meal.slot) ?? "meal";
  const name = formatRecipeDisplayName(
    stringValue(meal.display_name) ?? stringValue(meal.recipe_id) ?? "Recipe",
  );
  const scope = stringValue(meal.meal_scope) ?? "individual";
  const portionMultiplier = numberValue(
    meal.portion_multiplier ?? meal.portion_multiplier_member ?? meal.household_portion_sum,
  );
  const ingredients = getIngredients(meal);
  const steps = getCookingSteps(meal);
  const estimatedTime = getEstimatedTime(meal);
  const portionText =
    portionMultiplier !== null ? `${formatNumberWithDigits(portionMultiplier, 2)}x` : "1x";
  const timeText = estimatedTime ? `${estimatedTime} min` : "-";
  const sheetTitle = activeSheet === "alternatives" ? "Alternatives" : "How to cook";

  return (
    <View style={styles.container}>
      <View style={styles.headerRow}>
        <View style={styles.nameBlock}>
          <Text style={styles.slot}>{titleize(slot)}</Text>
          <Text style={styles.name}>{name}</Text>
        </View>
        <Text style={styles.scope}>{scope}</Text>
      </View>

      <View style={styles.dataTopRow}>
        <Text style={styles.dataTopValue}>{portionText}</Text>
        <Text style={styles.dataTopValue}>{timeText}</Text>
      </View>
      <View style={styles.macroRow}>
        <MacroMiniStat kind="calories" tone="soft" value={formatOptionalNumber(meal.kcal, 0)} />
        <MacroMiniStat kind="protein" tone="soft" value={formatOptionalNumber(meal.protein_g, 1)} />
        <MacroMiniStat kind="carbs" tone="soft" value={formatOptionalNumber(meal.carbs_g, 1)} />
        <MacroMiniStat kind="fat" tone="soft" value={formatOptionalNumber(meal.fat_g, 1)} />
      </View>

      <View style={styles.actionRow}>
        <SmallActionButton
          active={activeSheet === "cook"}
          label="Cook / Steps"
          onPress={() => setActiveSheet("cook")}
        />
        {recipeId ? (
          <SmallActionButton
            active={activeSheet === "alternatives"}
            label="Alternatives"
            onPress={() => setActiveSheet("alternatives")}
          />
        ) : null}
      </View>
      <BottomSheet
        onClose={() => setActiveSheet(null)}
        title={sheetTitle}
        titleAccessory={
          activeSheet === "cook" ? <Text style={styles.sheetTime}>{timeText}</Text> : undefined
        }
        visible={activeSheet !== null}
      >
        {activeSheet === "cook" ? (
          <View style={styles.sheetContent}>
            <SheetSection items={ingredients} showIngredientIcons title="Ingredients" />
            <View style={styles.sheetSection}>
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
        {activeSheet === "alternatives" && recipeId ? (
          <RecipeAlternativesPanel
            dayIndex={dayIndex}
            datasetProfile={datasetProfile}
            generationType="individual"
            householdId={householdId}
            isVisible={activeSheet === "alternatives"}
            memberProfile={memberProfile}
            memberProfileId={memberProfileId}
            onReplacementApplied={onReplacementApplied}
            planId={planId}
            replaceScope="individual_meal"
            shouldLoad
            slot={slot}
            sourceRecipeId={recipeId}
          />
        ) : null}
      </BottomSheet>
      {onSubmitFeedback ? (
        <MealFeedbackButtons
          disabled={feedbackDisabled}
          meal={meal}
          onSubmit={onSubmitFeedback}
          onUndo={onUndoFeedback}
          pendingFeedbackType={pendingFeedbackType}
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

function SheetSection({
  items,
  showIngredientIcons,
  title,
}: {
  items: string[];
  showIngredientIcons?: boolean;
  title: string;
}) {
  if (!items.length) {
    return null;
  }

  return (
    <View style={styles.sheetSection}>
      <Text style={styles.detailTitle}>{title}</Text>
      {items.map((item, index) => (
        <View key={`${item}-${index}`}>
          {showIngredientIcons ? (
            <IngredientMeasurementRow text={item} />
          ) : (
            <Text style={styles.detailText}>{item}</Text>
          )}
        </View>
      ))}
    </View>
  );
}

function formatOptionalNumber(value: unknown, digits: number): string {
  const parsed = numberValue(value);
  return parsed === null ? "-" : formatNumberWithDigits(parsed, digits);
}

function formatNumberWithDigits(value: number, digits: number): string {
  return value.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits > 0 ? 1 : 0,
  });
}

function numberValue(value: unknown): number | null {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return null;
  }
  return value;
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

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const styles = StyleSheet.create({
  alternativesButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 38,
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
  dataTopRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  dataTopValue: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "900",
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
  nameBlock: {
    flex: 1,
    gap: 2,
  },
  slot: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  name: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
  },
  macroRow: {
    alignItems: "center",
    flexDirection: "row",
    flexWrap: "nowrap",
    gap: 12,
    justifyContent: "space-between",
  },
  scope: {
    color: colors.accentDark,
    flexShrink: 0,
    fontSize: 11,
    fontWeight: "800",
    textAlign: "right",
    textTransform: "uppercase",
  },
  sheetContent: {
    gap: 12,
  },
  sheetSection: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 6,
    padding: 12,
  },
  sheetTime: {
    color: colors.accentDark,
    fontSize: 14,
    fontWeight: "900",
  },
});
