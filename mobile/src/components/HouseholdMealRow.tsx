import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import type {
  FeedbackType,
  HouseholdMeal,
  MealReplacementResponse,
  MealReplacementScope,
} from "../types/api";
import { colors } from "../theme/colors";
import { IngredientMeasurementRow } from "./IngredientMeasurementRow";
import { MealFeedbackButtons } from "./MealFeedbackButtons";
import { RecipeAlternativesPanel } from "./RecipeAlternativesPanel";
import { BottomSheet } from "./ui/BottomSheet";
import { MacroMiniStat } from "./ui/MacroMiniStat";

type HouseholdMealSheet = "cook" | "alternatives";

type HouseholdMealRowProps = {
  meal: HouseholdMeal;
  dayIndex?: number;
  datasetProfile?: string;
  householdId?: string;
  memberId?: string;
  planId?: string;
  feedbackDisabled?: boolean;
  pendingFeedbackType?: FeedbackType | null;
  onSubmitFeedback?: (meal: HouseholdMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onUndoFeedback?: (meal: HouseholdMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

export function HouseholdMealRow({
  meal,
  dayIndex = 1,
  datasetProfile,
  householdId,
  memberId,
  planId,
  feedbackDisabled,
  pendingFeedbackType,
  onSubmitFeedback,
  onUndoFeedback,
  onReplacementApplied,
}: HouseholdMealRowProps) {
  const [activeSheet, setActiveSheet] = useState<HouseholdMealSheet | null>(null);
  const name = stringValue(meal.display_name) ?? stringValue(meal.recipe_id) ?? "Meal";
  const slot = stringValue(meal.slot) ?? "meal";
  const scope = stringValue(meal.meal_scope) ?? "individual";
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
  const portionText =
    portionMultiplier !== null ? `${formatNumber(portionMultiplier, 2)}x` : "1x";
  const timeText = estimatedTime !== null ? `${Math.round(estimatedTime)} min` : "-";
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
            generationType="household"
            householdId={householdId}
            isVisible={activeSheet === "alternatives"}
            memberId={memberId}
            memberProfileId={memberId}
            onReplacementApplied={onReplacementApplied}
            planId={planId}
            replaceScope={replaceScope}
            shouldLoad
            slot={slot}
            sourceRecipeId={recipeId}
          />
        ) : null}
      </BottomSheet>
      {recipeId ? (
        <RecipeAlternativesPanel
          dayIndex={dayIndex}
          datasetProfile={datasetProfile}
          generationType="household"
          householdId={householdId}
          isVisible={false}
          memberId={memberId}
          memberProfileId={memberId}
          planId={planId}
          replaceScope={replaceScope}
          shouldLoad
          slot={slot}
          sourceRecipeId={recipeId}
        />
      ) : null}
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
    gap: 12,
    justifyContent: "space-between",
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
  macroRow: {
    alignItems: "center",
    flexDirection: "row",
    flexWrap: "nowrap",
    gap: 12,
    justifyContent: "space-between",
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
  slot: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
});
