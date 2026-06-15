import { StyleSheet, View } from "react-native";

import type {
  FeedbackType,
  GeneratedDay,
  GeneratedMeal,
  MealReplacementResponse,
} from "../types/api";
import { MealRow } from "./MealRow";
import { MacroMiniStat } from "./ui/MacroMiniStat";

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
  onSubmitFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onUndoFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => Promise<void> | void;
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
  onUndoFeedback,
  onReplacementApplied,
}: PlanDayCardProps) {
  const meals = getMealsFromGeneratedDay(day);
  const dayIndex = normalizeDayIndex(day.day_index);

  return (
    <View style={styles.container}>
      <DaySummaryStrip totals={day.totals} />
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
            onUndoFeedback={onUndoFeedback}
            onReplacementApplied={onReplacementApplied}
            pendingFeedbackType={getPendingFeedbackType?.(meal) ?? null}
            planId={planId}
          />
        ))}
      </View>
    </View>
  );
}

function DaySummaryStrip({
  totals,
}: {
  totals?: GeneratedDay["totals"];
}) {
  return (
    <View style={styles.summaryStrip}>
      <MacroMiniStat
        iconSize={18}
        kind="calories"
        tone="soft"
        value={`${formatNumber(totals?.kcal)} kcal`}
      />
      <MacroMiniStat
        iconSize={18}
        kind="protein"
        tone="soft"
        value={`${formatNumber(totals?.protein_g)}g`}
      />
      <MacroMiniStat
        iconSize={18}
        kind="carbs"
        tone="soft"
        value={`${formatNumber(totals?.carbs_g)}g`}
      />
      <MacroMiniStat
        iconSize={18}
        kind="fat"
        tone="soft"
        value={`${formatNumber(totals?.fat_g)}g`}
      />
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
  container: {
    gap: 12,
  },
  meals: {
    gap: 10,
  },
  summaryStrip: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: "#DDEAD3",
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
});
