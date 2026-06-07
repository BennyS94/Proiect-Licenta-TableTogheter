import { Pressable, StyleSheet, Text, View } from "react-native";

import type {
  DemoMemberProfile,
  HouseholdMacroTotals,
  HouseholdMeal,
  HouseholdMemberMacroSummary,
  HouseholdMemberMenu,
  HouseholdMemberTarget,
  HouseholdPlanGenerateResponse,
  MealReplacementResponse,
} from "../types/api";
import { colors } from "../theme/colors";
import { HouseholdMealRow } from "./HouseholdMealRow";

const MEAL_SLOT_ORDER = ["breakfast", "lunch", "snack", "dinner"];
const DISPLAY_DAY_INDEXES = [1, 2, 3, 4, 5];

type HouseholdMemberPlanViewProps = {
  memberId: string;
  datasetProfile?: string;
  householdId?: string;
  members: DemoMemberProfile[];
  onSelectDay: (dayIndex: number) => void;
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
  onReplacementApplied,
  plan,
  planId,
  selectedDayIndex,
}: HouseholdMemberPlanViewProps) {
  const dayIndexes = getAvailableDayIndexes(plan);
  const availableDayIndexes = new Set(dayIndexes);
  const menu = findMemberMenu(plan, memberId, selectedDayIndex);
  const meals = getMeals(menu);
  const target = findMemberTarget(plan, memberId);
  const summary = findMacroSummary(plan, memberId, selectedDayIndex);
  const totals = getTotals(menu, summary);
  const targetTotals = getTargetTotals(target, summary);

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

      <View style={styles.summaryBox}>
        <Text style={styles.summaryTitle}>Target summary</Text>
        <MetricLine label="kcal" value={formatOptionalNumber(targetTotals.kcal, 0)} />
        <MetricLine label="protein" value={formatOptionalWithUnit(targetTotals.protein_g, 1, "g")} />
        <MetricLine label="carbs" value={formatOptionalWithUnit(targetTotals.carbs_g, 1, "g")} />
        <MetricLine label="fats" value={formatOptionalWithUnit(targetTotals.fat_g, 1, "g")} />
      </View>

      <View style={styles.summaryBox}>
        <Text style={styles.summaryTitle}>Daily totals</Text>
        <MetricLine label="Kcal" value={formatOptionalNumber(totals.kcal, 0)} />
        <MetricLine label="Protein" value={formatOptionalWithUnit(totals.protein_g, 1, "g")} />
        <MetricLine label="Carbs" value={formatOptionalWithUnit(totals.carbs_g, 1, "g")} />
        <MetricLine label="Fat" value={formatOptionalWithUnit(totals.fat_g, 1, "g")} />
        {renderRatios(summary)}
      </View>

      <View style={styles.mealList}>
        {meals.length ? (
          meals.map((meal, index) => (
            <HouseholdMealRow
              dayIndex={selectedDayIndex}
              datasetProfile={datasetProfile}
              householdId={householdId}
              key={`${meal.slot ?? "meal"}-${meal.recipe_id ?? index}`}
              meal={meal}
              memberId={memberId}
              onReplacementApplied={onReplacementApplied}
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

function MetricLine({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.metricLine}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text style={styles.metricValue}>{value}</Text>
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

function findMemberTarget(
  plan: HouseholdPlanGenerateResponse,
  memberId: string,
): HouseholdMemberTarget | null {
  return (
    (plan.member_targets ?? []).find((target) => getMemberId(target) === memberId) ?? null
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

function getTargetTotals(
  target: HouseholdMemberTarget | null,
  summary: HouseholdMemberMacroSummary | null,
): HouseholdMacroTotals {
  const summaryTargets = asRecord(summary?.targets);
  return {
    kcal:
      numberValue(target?.target_kcal) ??
      numberValue(target?.kcal) ??
      numberValue(summaryTargets.target_kcal) ??
      numberValue(summaryTargets.kcal) ??
      numberValue(summary?.target_kcal) ??
      undefined,
    protein_g:
      numberValue(target?.target_protein_g) ??
      numberValue(target?.protein_g) ??
      numberValue(summaryTargets.target_protein_g) ??
      numberValue(summaryTargets.protein_g) ??
      numberValue(summary?.target_protein_g) ??
      undefined,
    carbs_g:
      numberValue(target?.target_carbs_g) ??
      numberValue(target?.carbs_g) ??
      numberValue(summaryTargets.target_carbs_g) ??
      numberValue(summaryTargets.carbs_g) ??
      numberValue(summary?.target_carbs_g) ??
      undefined,
    fat_g:
      numberValue(target?.target_fat_g) ??
      numberValue(target?.fat_g) ??
      numberValue(summaryTargets.target_fat_g) ??
      numberValue(summaryTargets.fat_g) ??
      numberValue(summary?.target_fat_g) ??
      undefined,
  };
}

function renderRatios(summary: HouseholdMemberMacroSummary | null) {
  const ratios = asRecord(summary?.ratios);
  const kcalRatio = numberValue(summary?.kcal_ratio) ?? numberValue(ratios.kcal);
  const proteinRatio = numberValue(summary?.protein_ratio) ?? numberValue(ratios.protein_g);
  if (kcalRatio === null && proteinRatio === null) {
    return null;
  }

  return (
    <>
      <MetricLine label="Kcal ratio" value={formatRatio(kcalRatio)} />
      <MetricLine label="Protein ratio" value={formatRatio(proteinRatio)} />
    </>
  );
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

function formatRatio(value: number | null): string {
  if (value === null) {
    return "-";
  }
  return `${Math.round(value * 100)}%`;
}

function formatOptionalNumber(value: unknown, digits: number): string {
  const parsed = numberValue(value);
  if (parsed === null) {
    return "Not available";
  }
  return parsed.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits > 0 ? 1 : 0,
  });
}

function formatOptionalWithUnit(value: unknown, digits: number, unit: string): string {
  const parsed = numberValue(value);
  if (parsed === null) {
    return "Not available";
  }
  return `${formatOptionalNumber(parsed, digits)}${unit}`;
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
  metricLabel: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "600",
  },
  metricLine: {
    flexDirection: "row",
    gap: 16,
    justifyContent: "space-between",
  },
  metricValue: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  mutedText: {
    color: colors.mutedSoft,
    fontSize: 15,
  },
  summaryBox: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 8,
    padding: 14,
  },
  summaryTitle: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
  },
  title: {
    color: colors.text,
    flex: 1,
    fontSize: 18,
    fontWeight: "800",
  },
});
