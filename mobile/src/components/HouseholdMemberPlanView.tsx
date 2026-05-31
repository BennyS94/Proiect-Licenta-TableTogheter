import { Pressable, StyleSheet, Text, View } from "react-native";

import type {
  DemoMemberProfile,
  HouseholdMacroTotals,
  HouseholdMeal,
  HouseholdMemberMacroSummary,
  HouseholdMemberMenu,
  HouseholdMemberTarget,
  HouseholdPlanGenerateResponse,
} from "../types/api";
import { HouseholdMealRow } from "./HouseholdMealRow";

type HouseholdMemberPlanViewProps = {
  memberId: string;
  members: DemoMemberProfile[];
  onSelectDay: (dayIndex: number) => void;
  plan: HouseholdPlanGenerateResponse;
  selectedDayIndex: number;
};

export function HouseholdMemberPlanView({
  memberId,
  members,
  onSelectDay,
  plan,
  selectedDayIndex,
}: HouseholdMemberPlanViewProps) {
  const dayIndexes = getAvailableDayIndexes(plan);
  const memberName = getMemberDisplayName(memberId, members, plan.selected_members ?? []);
  const menu = findMemberMenu(plan, memberId, selectedDayIndex);
  const meals = getMeals(menu);
  const target = findMemberTarget(plan, memberId);
  const summary = findMacroSummary(plan, memberId, selectedDayIndex);
  const totals = getTotals(menu, summary);
  const targetTotals = getTargetTotals(target, summary);

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <Text style={styles.title}>{memberName}</Text>
        <Text style={styles.meta}>Day {selectedDayIndex}</Text>
      </View>

      <View style={styles.daySelector}>
        {dayIndexes.map((dayIndex) => (
          <Pressable
            accessibilityRole="button"
            key={`${dayIndex}`}
            onPress={() => onSelectDay(dayIndex)}
            style={({ pressed }) => [
              styles.dayButton,
              dayIndex === selectedDayIndex ? styles.dayButtonActive : null,
              pressed ? styles.dayButtonPressed : null,
            ]}
          >
            <Text
              style={[
                styles.dayButtonText,
                dayIndex === selectedDayIndex ? styles.dayButtonTextActive : null,
              ]}
            >
              Day {dayIndex}
            </Text>
          </Pressable>
        ))}
      </View>

      <View style={styles.summaryBox}>
        <Text style={styles.summaryTitle}>Target summary</Text>
        <MetricLine label="Target kcal" value={formatOptionalNumber(targetTotals.kcal, 0)} />
        <MetricLine label="Protein" value={formatOptionalWithUnit(targetTotals.protein_g, 1, "g")} />
        <MetricLine label="Carbs" value={formatOptionalWithUnit(targetTotals.carbs_g, 1, "g")} />
        <MetricLine label="Fat" value={formatOptionalWithUnit(targetTotals.fat_g, 1, "g")} />
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
              key={`${meal.slot ?? "meal"}-${meal.recipe_id ?? index}`}
              meal={meal}
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
  return (meals ?? []).filter(isRecord) as HouseholdMeal[];
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
      numberValue(summaryTargets.kcal) ??
      numberValue(summary?.target_kcal) ??
      undefined,
    protein_g:
      numberValue(target?.protein_g) ?? numberValue(summaryTargets.protein_g) ?? undefined,
    carbs_g: numberValue(target?.carbs_g) ?? numberValue(summaryTargets.carbs_g) ?? undefined,
    fat_g: numberValue(target?.fat_g) ?? numberValue(summaryTargets.fat_g) ?? undefined,
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
    return "-";
  }
  return parsed.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits > 0 ? 1 : 0,
  });
}

function formatOptionalWithUnit(value: unknown, digits: number, unit: string): string {
  const parsed = numberValue(value);
  if (parsed === null) {
    return "-";
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

const styles = StyleSheet.create({
  container: {
    gap: 14,
  },
  dayButton: {
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  dayButtonActive: {
    backgroundColor: "#165D77",
  },
  dayButtonPressed: {
    opacity: 0.82,
  },
  dayButtonText: {
    color: "#165D77",
    fontSize: 14,
    fontWeight: "800",
  },
  dayButtonTextActive: {
    color: "#FFFFFF",
  },
  daySelector: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
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
    color: "#6B7280",
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  metricLabel: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "600",
  },
  metricLine: {
    flexDirection: "row",
    gap: 16,
    justifyContent: "space-between",
  },
  metricValue: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  mutedText: {
    color: "#6B7280",
    fontSize: 15,
  },
  summaryBox: {
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    gap: 8,
    padding: 14,
  },
  summaryTitle: {
    color: "#111827",
    fontSize: 15,
    fontWeight: "800",
  },
  title: {
    color: "#111827",
    flex: 1,
    fontSize: 18,
    fontWeight: "800",
  },
});
