import { Pressable, StyleSheet, Text, View } from "react-native";

import type {
  DemoMemberProfile,
  FeedbackType,
  HouseholdMacroTotals,
  HouseholdMeal,
  HouseholdMemberMacroSummary,
  HouseholdMemberMenu,
  HouseholdPlanGenerateResponse,
  MealReplacementResponse,
} from "../types/api";
import { colors } from "../theme/colors";
import { HouseholdMealRow } from "./HouseholdMealRow";
import { MacroMiniStat } from "./ui/MacroMiniStat";

const MEAL_SLOT_ORDER = ["breakfast", "lunch", "snack", "dinner"];
const DISPLAY_DAY_INDEXES = [1, 2, 3, 4, 5];

type HouseholdMemberPlanViewProps = {
  memberId: string;
  datasetProfile?: string;
  householdId?: string;
  members: DemoMemberProfile[];
  onSelectDay: (dayIndex: number) => void;
  feedbackDisabled?: boolean;
  getPendingFeedbackType?: (meal: HouseholdMeal) => FeedbackType | null;
  onSubmitFeedback?: (meal: HouseholdMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onUndoFeedback?: (meal: HouseholdMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
  plan: HouseholdPlanGenerateResponse;
  planId?: string;
  selectedDayIndex: number;
};

export function HouseholdMemberPlanView({
  memberId,
  datasetProfile,
  householdId,
  members,
  onSelectDay,
  feedbackDisabled,
  getPendingFeedbackType,
  onSubmitFeedback,
  onUndoFeedback,
  onReplacementApplied,
  plan,
  planId,
  selectedDayIndex,
}: HouseholdMemberPlanViewProps) {
  const dayIndexes = getAvailableDayIndexes(plan);
  const availableDayIndexes = new Set(dayIndexes);
  const menu = findMemberMenu(plan, memberId, selectedDayIndex);
  const meals = getMeals(menu);
  const summary = findMacroSummary(plan, memberId, selectedDayIndex);
  const totals = getTotals(menu, summary);

  return (
    <View style={styles.container}>
      <View style={styles.daySelector}>
        {DISPLAY_DAY_INDEXES.map((dayIndex) => {
          const isAvailable = availableDayIndexes.has(dayIndex);
          return (
          <Pressable
            accessibilityRole="button"
            disabled={!isAvailable}
            key={`${dayIndex}`}
            onPress={() => onSelectDay(dayIndex)}
            style={({ pressed }) => [
              styles.dayButton,
              dayIndex === selectedDayIndex ? styles.dayButtonActive : null,
              !isAvailable ? styles.dayButtonDisabled : null,
              pressed && isAvailable ? styles.dayButtonPressed : null,
            ]}
          >
            <Text
              style={[
                styles.dayButtonText,
                dayIndex === selectedDayIndex ? styles.dayButtonTextActive : null,
                !isAvailable ? styles.dayButtonTextDisabled : null,
              ]}
            >
              Day {dayIndex}
            </Text>
          </Pressable>
          );
        })}
      </View>

      <View style={styles.summaryStrip}>
        <MacroMiniStat
          iconSize={18}
          kind="calories"
          tone="soft"
          value={`${formatOptionalNumber(totals.kcal, 0)} kcal`}
        />
        <MacroMiniStat
          iconSize={18}
          kind="protein"
          tone="soft"
          value={`${formatOptionalNumber(totals.protein_g, 0)}g`}
        />
        <MacroMiniStat
          iconSize={18}
          kind="carbs"
          tone="soft"
          value={`${formatOptionalNumber(totals.carbs_g, 0)}g`}
        />
        <MacroMiniStat
          iconSize={18}
          kind="fat"
          tone="soft"
          value={`${formatOptionalNumber(totals.fat_g, 0)}g`}
        />
      </View>

      <View style={styles.mealList}>
        {meals.length ? (
          meals.map((meal, index) => (
            <HouseholdMealRow
              dayIndex={selectedDayIndex}
              datasetProfile={datasetProfile}
              feedbackDisabled={feedbackDisabled}
              householdId={householdId}
              key={`${meal.slot ?? "meal"}-${meal.recipe_id ?? index}`}
              meal={meal}
              memberId={memberId}
              onSubmitFeedback={onSubmitFeedback}
              onUndoFeedback={onUndoFeedback}
              onReplacementApplied={onReplacementApplied}
              pendingFeedbackType={getPendingFeedbackType?.(meal) ?? null}
              planId={planId}
            />
          ))
        ) : (
          <Text style={styles.mutedText}>No meals returned for this member/day.</Text>
        )}
      </View>
    </View>
  );
}

function getAvailableDayIndexes(plan: HouseholdPlanGenerateResponse): number[] {
  const dayCount = numberValue(plan.days);
  if (dayCount !== null && dayCount > 0) {
    return Array.from({ length: Math.round(dayCount) }, (_, index) => index + 1);
  }

  const rawIndexes = [
    ...(plan.daily_plan ?? []).map((day) => day.day_index),
    ...(plan.per_member_menus ?? []).map((menu) => menu.day_index ?? menu.day),
  ];
  const indexes = rawIndexes
    .map((value) => normalizeDayIndex(numberValue(value)))
    .filter((value): value is number => value !== null);
  const uniqueIndexes = [...new Set(indexes)].sort((left, right) => left - right);
  return uniqueIndexes.length ? uniqueIndexes : [1];
}

function findMemberMenu(
  plan: HouseholdPlanGenerateResponse,
  memberId: string,
  selectedDayIndex: number,
): HouseholdMemberMenu | null {
  return (
    (plan.per_member_menus ?? []).find((menu) => {
      const menuMemberId = getMemberId(menu);
      const menuDayIndex = normalizeDayIndex(numberValue(menu.day_index ?? menu.day));
      return menuMemberId === memberId && menuDayIndex === selectedDayIndex;
    }) ?? null
  );
}

function findMacroSummary(
  plan: HouseholdPlanGenerateResponse,
  memberId: string,
  selectedDayIndex: number,
): HouseholdMemberMacroSummary | null {
  return (
    (plan.member_macro_summaries ?? []).find((summary) => {
      const summaryMemberId = getMemberId(summary);
      const summaryDayIndex = normalizeDayIndex(numberValue(summary.day_index ?? summary.day));
      return summaryMemberId === memberId && summaryDayIndex === selectedDayIndex;
    }) ?? null
  );
}

function getMeals(menu: HouseholdMemberMenu | null): HouseholdMeal[] {
  const meals = Array.isArray(menu?.meals) ? menu?.meals : menu?.selected_meals;
  return sortMealsBySlot((meals ?? []).filter(isRecord) as HouseholdMeal[]);
}

function getTotals(
  menu: HouseholdMemberMenu | null,
  summary: HouseholdMemberMacroSummary | null,
): HouseholdMacroTotals {
  const menuTotals = firstRecord(menu?.totals, menu?.daily_totals, menu?.macro_totals);
  const summaryTotals = asRecord(summary?.totals);
  return {
    kcal:
      numberValue(menuTotals.kcal) ??
      numberValue(summaryTotals.kcal) ??
      numberValue(summary?.kcal) ??
      undefined,
    protein_g:
      numberValue(menuTotals.protein_g) ??
      numberValue(summaryTotals.protein_g) ??
      numberValue(summary?.protein_g) ??
      undefined,
    carbs_g:
      numberValue(menuTotals.carbs_g) ??
      numberValue(summaryTotals.carbs_g) ??
      numberValue(summary?.carbs_g) ??
      undefined,
    fat_g:
      numberValue(menuTotals.fat_g) ??
      numberValue(summaryTotals.fat_g) ??
      numberValue(summary?.fat_g) ??
      undefined,
  };
}

function getMemberDisplayName(
  memberId: string,
  selectedMembers: DemoMemberProfile[],
  responseMembers: DemoMemberProfile[],
): string {
  const member = [...selectedMembers, ...responseMembers].find(
    (candidate) => getMemberId(candidate) === memberId,
  );
  return String(
    member?.display_name ?? member?.profile_name ?? member?.member_profile_id ?? memberId,
  );
}

function getMemberId(value: {
  member_id?: string;
  member_profile_id?: string;
  [key: string]: unknown;
}): string {
  return String(value.member_profile_id ?? value.member_id ?? "").trim();
}

function normalizeDayIndex(value: number | null): number | null {
  if (value === null) {
    return null;
  }
  return value <= 0 ? value + 1 : value;
}

function formatOptionalNumber(value: unknown, digits: number): string {
  const parsed = numberValue(value);
  if (parsed === null) {
    return "-";
  }
  return parsed.toLocaleString("en-US", {
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function firstRecord(...values: unknown[]): Record<string, unknown> {
  for (const value of values) {
    if (isRecord(value) && Object.keys(value).length > 0) {
      return value;
    }
  }
  return {};
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

const styles = StyleSheet.create({
  container: {
    gap: 14,
  },
  dayButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minWidth: 0,
    paddingHorizontal: 4,
    paddingVertical: 8,
  },
  dayButtonActive: {
    backgroundColor: colors.accent,
  },
  dayButtonDisabled: {
    backgroundColor: "#F3F4F6",
    borderColor: "#D1D5DB",
  },
  dayButtonPressed: {
    opacity: 0.82,
  },
  dayButtonText: {
    color: colors.accent,
    fontSize: 12,
    fontWeight: "800",
    textAlign: "center",
  },
  dayButtonTextActive: {
    color: "#FFFFFF",
  },
  dayButtonTextDisabled: {
    color: "#9CA3AF",
  },
  daySelector: {
    flexDirection: "row",
    gap: 6,
    width: "100%",
  },
  header: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  mealList: {
    gap: 10,
  },
  meta: {
    color: colors.mutedSoft,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  mutedText: {
    color: colors.mutedSoft,
    fontSize: 15,
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
  title: {
    color: colors.text,
    flex: 1,
    fontSize: 18,
    fontWeight: "800",
  },
});
