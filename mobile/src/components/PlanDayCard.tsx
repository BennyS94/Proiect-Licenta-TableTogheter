import { StyleSheet, Text, View } from "react-native";

import type {
  FeedbackType,
  GeneratedDay,
  GeneratedMeal,
  MealReplacementResponse,
} from "../types/api";
import { colors } from "../theme/colors";
import { MealRow } from "./MealRow";

const MEAL_SLOT_ORDER = ["breakfast", "lunch", "snack", "dinner"];

type PlanDayCardProps = {
  day: GeneratedDay;
  datasetProfile?: string;
  householdId?: string;
  memberProfileId?: string;
  memberProfile?: Record<string, unknown>;
  planId?: string;
  feedbackDisabled?: boolean;
  getPendingFeedbackType?: (meal: GeneratedMeal) => FeedbackType | null;
  onSubmitFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => void;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

export function PlanDayCard({
  day,
  datasetProfile,
  householdId,
  memberProfileId,
  memberProfile,
  planId,
  feedbackDisabled,
  getPendingFeedbackType,
  onSubmitFeedback,
  onReplacementApplied,
}: PlanDayCardProps) {
  const meals = getMealsFromGeneratedDay(day);
  const dayIndex = normalizeDayIndex(day.day_index);

  return (
    <View style={styles.card}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>Day {day.day_index ?? 1}</Text>
        <Text style={styles.totals}>
          {formatNumber(day.totals?.kcal)} kcal · {formatNumber(day.totals?.protein_g)}g protein
        </Text>
      </View>
      <View style={styles.meals}>
        {meals.map((meal, index) => (
          <MealRow
            dayIndex={dayIndex}
            datasetProfile={datasetProfile}
            feedbackDisabled={feedbackDisabled}
            householdId={householdId}
            key={`${meal.slot ?? "meal"}-${meal.recipe_id ?? index}`}
            meal={meal}
            memberProfile={memberProfile}
            memberProfileId={memberProfileId}
            onSubmitFeedback={onSubmitFeedback}
            onReplacementApplied={onReplacementApplied}
            pendingFeedbackType={getPendingFeedbackType?.(meal) ?? null}
            planId={planId}
          />
        ))}
      </View>
    </View>
  );
}

function normalizeDayIndex(value: unknown): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return 1;
  }
  return value <= 0 ? value + 1 : value;
}

function formatNumber(value: unknown): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return String(Math.round(value));
}

function getMealsFromGeneratedDay(day: GeneratedDay): GeneratedMeal[] {
  if (Array.isArray(day.selected_meals)) {
    return sortMealsBySlot(day.selected_meals.filter(isGeneratedMeal));
  }
  if (Array.isArray(day.meals)) {
    return sortMealsBySlot(day.meals.filter(isGeneratedMeal));
  }
  return [];
}

function sortMealsBySlot<T extends { slot?: unknown }>(meals: T[]): T[] {
  return [...meals].sort((left, right) => {
    const leftIndex = getMealSlotOrderIndex(left.slot);
    const rightIndex = getMealSlotOrderIndex(right.slot);
    if (leftIndex !== rightIndex) {
      return leftIndex - rightIndex;
    }
    return String(left.slot ?? "").localeCompare(String(right.slot ?? ""));
  });
}

function getMealSlotOrderIndex(slot: unknown): number {
  const normalized = String(slot ?? "").trim().toLowerCase();
  const index = MEAL_SLOT_ORDER.indexOf(normalized);
  return index >= 0 ? index : MEAL_SLOT_ORDER.length;
}

function isGeneratedMeal(value: unknown): value is GeneratedMeal {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const styles = StyleSheet.create({
  card: {
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 8,
    backgroundColor: colors.card,
    padding: 16,
    gap: 12,
  },
  headerRow: {
    gap: 4,
  },
  title: {
    color: colors.text,
    fontSize: 17,
    fontWeight: "800",
  },
  totals: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
  },
  meals: {
    gap: 10,
  },
});
