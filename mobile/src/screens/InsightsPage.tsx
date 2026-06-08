import type { ComponentType, ReactNode } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import { MacroDonutRing } from "../components/insights/MacroDonutRing";
import {
  BreakfastSunriseIcon,
  DinnerMoonIcon,
  LunchSunIcon,
} from "../components/icons/MealTimeIcons";
import {
  CaloriesFlameIcon,
  CarbsWheatIcon,
  FatsAvocadoIcon,
  ProteinDrumstickIcon,
} from "../components/icons/MacroNutrientIcons";
import { AppScreen } from "../components/ui/AppScreen";
import { EmptyState } from "../components/ui/EmptyState";
import { colors } from "../theme/colors";

export type InsightsDaySelection = number | "average";

export type InsightsTotals = {
  carbs_g?: number;
  fat_g?: number;
  kcal?: number;
  protein_g?: number;
};

export type MealContribution = {
  kcal?: number;
  protein_g?: number;
  slot: string;
};

type InsightsPageProps = {
  activeProfileMeta: string;
  activeProfileName: string;
  dayIndexes: number[];
  hasMembers: boolean;
  hasPlan: boolean;
  householdName: string;
  isSetupComplete: boolean;
  mealContributions: MealContribution[];
  onGoToHousehold: () => void;
  onGoToMealPlan: () => void;
  onSelectDay: (value: InsightsDaySelection) => void;
  profileSelector?: ReactNode;
  selectedDay: InsightsDaySelection;
  targetTotals?: InsightsTotals;
  totals?: InsightsTotals;
};

type IconComponent = ComponentType<{ size?: number }>;

const MACRO_COLORS = {
  calories: colors.accent,
  carbs: "#F28A12",
  fat: "#F5C400",
  protein: "#EF3B45",
};

const MEAL_DASHBOARD_ORDER = ["breakfast", "lunch", "dinner"];

export function InsightsPage({
  activeProfileMeta,
  activeProfileName,
  dayIndexes,
  hasMembers,
  hasPlan,
  isSetupComplete,
  mealContributions,
  onGoToHousehold,
  onGoToMealPlan,
  onSelectDay,
  profileSelector,
  selectedDay,
  targetTotals,
  totals,
}: InsightsPageProps) {
  if (!isSetupComplete) {
    return (
      <AppScreen>
        <EmptyState
          actionLabel="Go to Account Setup"
          onAction={onGoToHousehold}
          text="Create an account to start planning meals for your household."
          title="Set up your account first"
        />
      </AppScreen>
    );
  }

  if (!hasMembers) {
    return (
      <AppScreen>
        <EmptyState
          actionLabel="Add Member Profile"
          onAction={onGoToHousehold}
          text="Insights are based on generated meal plans and nutrition targets. Add a member profile first."
          title="No profile data available"
        />
      </AppScreen>
    );
  }

  if (!hasPlan) {
    return (
      <AppScreen>
        <EmptyState
          actionLabel="Go to Meal Plan"
          onAction={onGoToMealPlan}
          text="Generate a meal plan first to see calorie, macro and meal contribution insights."
          title="No insights yet"
        />
      </AppScreen>
    );
  }

  const macroPercents = macroEnergyPercents(totals);
  const selectedDayLabel = formatDayLabel(selectedDay);
  const mealRows = getDashboardMealRows(mealContributions);
  const compactMeta = compactProfileMeta(activeProfileMeta);

  return (
    <AppScreen contentContainerStyle={styles.screenContainer}>
      <View style={styles.header}>
        <Text style={styles.title}>Insights</Text>
        {profileSelector ? (
          <View style={styles.profileSelectorSlot}>{profileSelector}</View>
        ) : (
          <View style={styles.profileFallback}>
            <Text numberOfLines={1} style={styles.profileFallbackName}>
              {activeProfileName}
            </Text>
            {compactMeta ? (
              <Text numberOfLines={1} style={styles.profileFallbackMeta}>
                {compactMeta}
              </Text>
            ) : null}
          </View>
        )}
      </View>

      <InsightsDaySelector
        dayIndexes={dayIndexes}
        onSelect={onSelectDay}
        selected={selectedDay}
      />

      <View style={styles.dashboardCard}>
        <View style={styles.cardHeader}>
          <Text style={styles.cardTitle}>Daily Balance</Text>
          <Text style={styles.dayPill}>{selectedDayLabel}</Text>
        </View>
        <View style={styles.balanceContent}>
          <MacroDonutRing
            carbsPercent={macroPercents.carbs}
            fatPercent={macroPercents.fat}
            proteinPercent={macroPercents.protein}
            size={132}
          />
          <View style={styles.balanceSummary}>
            <Text numberOfLines={1} style={styles.kcalValue}>
              {formatNumber(totals?.kcal)} kcal
            </Text>
            <Text style={styles.targetText}>Daily total</Text>
            <View style={styles.legendList}>
              <MacroLegendRow
                color={MACRO_COLORS.protein}
                label="Protein"
                percent={macroPercents.protein}
              />
              <MacroLegendRow
                color={MACRO_COLORS.carbs}
                label="Carbs"
                percent={macroPercents.carbs}
              />
              <MacroLegendRow
                color={MACRO_COLORS.fat}
                label="Fats"
                percent={macroPercents.fat}
              />
            </View>
          </View>
        </View>
      </View>

      <View style={styles.dashboardCard}>
        <View style={styles.cardHeader}>
          <Text style={styles.cardTitle}>Macro Targets</Text>
        </View>
        <View style={styles.targetList}>
          <MacroTargetRow
            Icon={CaloriesFlameIcon}
            actual={totals?.kcal}
            color={MACRO_COLORS.calories}
            label="Calories"
            target={targetTotals?.kcal}
            unit="kcal"
          />
          <MacroTargetRow
            Icon={ProteinDrumstickIcon}
            actual={totals?.protein_g}
            color={MACRO_COLORS.protein}
            label="Protein"
            target={targetTotals?.protein_g}
            unit="g"
          />
          <MacroTargetRow
            Icon={CarbsWheatIcon}
            actual={totals?.carbs_g}
            color={MACRO_COLORS.carbs}
            label="Carbs"
            target={targetTotals?.carbs_g}
            unit="g"
          />
          <MacroTargetRow
            Icon={FatsAvocadoIcon}
            actual={totals?.fat_g}
            color={MACRO_COLORS.fat}
            label="Fats"
            target={targetTotals?.fat_g}
            unit="g"
          />
        </View>
      </View>

      <View style={styles.dashboardCard}>
        <View style={styles.cardHeader}>
          <Text style={styles.cardTitle}>Meal Contribution</Text>
        </View>
        <View style={styles.mealList}>
          {mealRows.length ? (
            mealRows.map((meal) => (
              <MealContributionRow key={meal.slot} meal={meal} />
            ))
          ) : (
            <Text style={styles.emptyDashboardText}>
              No meal breakdown available for this day.
            </Text>
          )}
        </View>
      </View>

      <View style={styles.dashboardCard}>
        <View style={styles.cardHeader}>
          <Text style={styles.cardTitle}>Micronutrient Snapshot</Text>
          <Text style={styles.conceptBadge}>Concept / Estimated</Text>
        </View>
        <Text style={styles.bodyText}>
          Limited micronutrient data available for this demo.
        </Text>
      </View>
    </AppScreen>
  );
}

function InsightsDaySelector({
  dayIndexes,
  onSelect,
  selected,
}: {
  dayIndexes: number[];
  onSelect: (value: InsightsDaySelection) => void;
  selected: InsightsDaySelection;
}) {
  const generatedDays = new Set(dayIndexes);
  const options: InsightsDaySelection[] = [1, 2, 3, 4, 5, "average"];

  return (
    <View style={styles.daySelector}>
      {options.map((option) => {
        const isAverage = option === "average";
        const enabled = isAverage ? dayIndexes.length > 0 : generatedDays.has(option);
        const active = selected === option;
        const label = isAverage ? "Avg" : `Day ${option}`;
        return (
          <Pressable
            accessibilityRole="button"
            disabled={!enabled}
            key={String(option)}
            onPress={() => onSelect(option)}
            style={({ pressed }) => [
              styles.dayChip,
              active ? styles.dayChipActive : null,
              !active && enabled ? styles.dayChipEnabled : null,
              !enabled ? styles.dayChipDisabled : null,
              pressed && enabled ? styles.pressed : null,
            ]}
          >
            <Text
              numberOfLines={1}
              style={[
                styles.dayChipText,
                active ? styles.dayChipTextActive : null,
                !enabled ? styles.dayChipTextDisabled : null,
              ]}
            >
              {label}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

function MacroLegendRow({
  color,
  label,
  percent,
}: {
  color: string;
  label: string;
  percent: number;
}) {
  return (
    <View style={styles.legendRow}>
      <View style={[styles.legendDot, { backgroundColor: color }]} />
      <Text style={styles.legendLabel}>{label}</Text>
      <Text style={styles.legendValue}>{formatPercent(percent)}</Text>
    </View>
  );
}

function MacroTargetRow({
  Icon,
  actual,
  color,
  label,
  target,
  unit,
}: {
  Icon: IconComponent;
  actual?: number;
  color: string;
  label: string;
  target?: number;
  unit: string;
}) {
  const percent = percentOfTarget(actual, target);
  const cappedPercent = Math.min(100, Math.max(0, percent ?? 0));
  const progressWidth = `${cappedPercent}%` as `${number}%`;

  return (
    <View style={styles.targetRow}>
      <View style={styles.targetTopLine}>
        <View style={styles.targetIdentity}>
          <View style={styles.macroIconShell}>
            <Icon size={25} />
          </View>
          <Text style={styles.targetLabel}>{label}</Text>
        </View>
        <Text numberOfLines={1} style={styles.targetValue}>
          {formatNumber(actual)} / {target == null ? "-" : formatNumber(target)} {unit}
        </Text>
      </View>
      <View style={styles.targetProgressLine}>
        <View style={styles.progressTrack}>
          <View
            style={[
              styles.progressFill,
              { backgroundColor: color, width: progressWidth },
            ]}
          />
        </View>
        <Text style={styles.targetPercent}>{percent === null ? "-" : `${Math.round(percent)}%`}</Text>
      </View>
    </View>
  );
}

function MealContributionRow({ meal }: { meal: MealContribution }) {
  const normalizedSlot = normalizeSlot(meal.slot);
  const Icon = getMealIcon(normalizedSlot);

  return (
    <View style={styles.mealRow}>
      <View style={styles.mealIconShell}>
        <Icon size={23} />
      </View>
      <View style={styles.mealTextBlock}>
        <Text style={styles.mealSlot}>{formatMealSlot(normalizedSlot)}</Text>
        <Text style={styles.mealValue}>
          {formatNumber(meal.kcal)} kcal {"\u00B7"} {formatNumber(meal.protein_g)}g protein
        </Text>
      </View>
    </View>
  );
}

function getMealIcon(slot: string): IconComponent {
  if (slot === "breakfast") {
    return BreakfastSunriseIcon;
  }
  if (slot === "dinner") {
    return DinnerMoonIcon;
  }
  return LunchSunIcon;
}

function getDashboardMealRows(meals: MealContribution[]): MealContribution[] {
  const bySlot = new Map<string, MealContribution>();
  for (const meal of meals) {
    bySlot.set(normalizeSlot(meal.slot), meal);
  }
  const dashboardRows = MEAL_DASHBOARD_ORDER
    .map((slot) => bySlot.get(slot))
    .filter((meal): meal is MealContribution => Boolean(meal));
  return dashboardRows.length ? dashboardRows : meals.slice(0, 3);
}

function macroEnergyPercents(totals?: InsightsTotals) {
  const protein = numberValue(totals?.protein_g) * 4;
  const carbs = numberValue(totals?.carbs_g) * 4;
  const fat = numberValue(totals?.fat_g) * 9;
  const total = protein + carbs + fat;
  if (total <= 0) {
    return { carbs: 0, fat: 0, protein: 0 };
  }
  return {
    carbs: (carbs / total) * 100,
    fat: (fat / total) * 100,
    protein: (protein / total) * 100,
  };
}

function percentOfTarget(actual?: number, target?: number): number | null {
  const parsedActual = numberValue(actual);
  const parsedTarget = numberValue(target);
  if (parsedTarget <= 0) {
    return null;
  }
  return (parsedActual / parsedTarget) * 100;
}

function compactProfileMeta(meta?: string): string | null {
  if (!meta) {
    return null;
  }
  const cleaned = meta
    .replace(/^Goal:\s*/i, "")
    .replace(/\s+\+\s+snack\b/i, "")
    .trim();
  const parts = cleaned
    .split(/\s+-\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length >= 2) {
    return `${parts[0]} \u00B7 ${parts[1]}`;
  }
  return cleaned || null;
}

function formatDayLabel(value: InsightsDaySelection): string {
  return value === "average" ? "Avg" : `Day ${value}`;
}

function formatMealSlot(value: string): string {
  if (value === "breakfast") {
    return "Breakfast";
  }
  if (value === "lunch") {
    return "Lunch";
  }
  if (value === "dinner") {
    return "Dinner";
  }
  return titleize(value);
}

function formatNumber(value: unknown): string {
  const parsed = numberValue(value);
  if (parsed === 0 && value == null) {
    return "-";
  }
  return parsed ? String(Math.round(parsed)) : parsed === 0 ? "0" : "-";
}

function formatPercent(value: number): string {
  return `${Math.round(numberValue(value))}%`;
}

function numberValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function normalizeSlot(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, "_");
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const styles = StyleSheet.create({
  balanceContent: {
    alignItems: "center",
    flexDirection: "row",
    gap: 14,
  },
  balanceSummary: {
    flex: 1,
    gap: 8,
    minWidth: 0,
  },
  bodyText: {
    color: colors.muted,
    fontSize: 15,
    fontWeight: "600",
    lineHeight: 21,
  },
  cardHeader: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  cardTitle: {
    color: "#1B2430",
    flexShrink: 1,
    fontSize: 22,
    fontWeight: "900",
    lineHeight: 27,
  },
  conceptBadge: {
    backgroundColor: "#F1F8EA",
    borderColor: "#DDEFCF",
    borderRadius: 999,
    borderWidth: 1,
    color: colors.accentDark,
    fontSize: 12,
    fontWeight: "900",
    overflow: "hidden",
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  dashboardCard: {
    backgroundColor: colors.card,
    borderColor: "#E0E7DA",
    borderRadius: 24,
    borderWidth: 1,
    elevation: 2,
    gap: 16,
    paddingHorizontal: 18,
    paddingVertical: 18,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 7 },
    shadowOpacity: 0.08,
    shadowRadius: 14,
  },
  dayChip: {
    alignItems: "center",
    borderRadius: 14,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 34,
    minWidth: 0,
    paddingHorizontal: 4,
  },
  dayChipActive: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  dayChipDisabled: {
    backgroundColor: "#EEF1EA",
    borderColor: "#E1E5DC",
  },
  dayChipEnabled: {
    backgroundColor: colors.card,
    borderColor: colors.accent,
  },
  dayChipText: {
    color: colors.accentDark,
    fontSize: 12,
    fontWeight: "900",
    lineHeight: 15,
  },
  dayChipTextActive: {
    color: "#FFFFFF",
  },
  dayChipTextDisabled: {
    color: "#9CA3AF",
  },
  dayPill: {
    backgroundColor: "#F1F8EA",
    borderColor: "#DDEFCF",
    borderRadius: 999,
    borderWidth: 1,
    color: colors.accentDark,
    fontSize: 13,
    fontWeight: "900",
    overflow: "hidden",
    paddingHorizontal: 11,
    paddingVertical: 5,
  },
  daySelector: {
    flexDirection: "row",
    gap: 6,
  },
  emptyDashboardText: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 20,
  },
  header: {
    gap: 12,
    paddingTop: 2,
  },
  kcalValue: {
    color: "#1B2430",
    fontSize: 21,
    fontWeight: "900",
    lineHeight: 26,
  },
  legendDot: {
    borderRadius: 999,
    height: 9,
    width: 9,
  },
  legendLabel: {
    color: colors.muted,
    flex: 1,
    fontSize: 14,
    fontWeight: "800",
  },
  legendList: {
    gap: 6,
    paddingTop: 2,
  },
  legendRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  legendValue: {
    color: "#1B2430",
    fontSize: 14,
    fontWeight: "900",
    textAlign: "right",
  },
  macroIconShell: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: "#E1ECD8",
    borderRadius: 14,
    borderWidth: 1,
    height: 34,
    justifyContent: "center",
    width: 34,
  },
  mealIconShell: {
    alignItems: "center",
    backgroundColor: "#EEF8E6",
    borderRadius: 999,
    height: 40,
    justifyContent: "center",
    width: 40,
  },
  mealList: {
    gap: 12,
  },
  mealRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
  },
  mealSlot: {
    color: "#1B2430",
    fontSize: 16,
    fontWeight: "900",
    lineHeight: 20,
  },
  mealTextBlock: {
    flex: 1,
    gap: 2,
    minWidth: 0,
  },
  mealValue: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 19,
  },
  pressed: {
    opacity: 0.82,
  },
  profileFallback: {
    alignItems: "center",
    backgroundColor: colors.card,
    borderColor: "#DDEAD3",
    borderRadius: 18,
    borderWidth: 1,
    gap: 2,
    minHeight: 56,
    paddingHorizontal: 16,
    paddingVertical: 8,
  },
  profileFallbackMeta: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "700",
    lineHeight: 16,
  },
  profileFallbackName: {
    color: "#1B2430",
    fontSize: 17,
    fontWeight: "900",
    lineHeight: 21,
  },
  profileSelectorSlot: {
    gap: 0,
  },
  progressFill: {
    borderRadius: 999,
    height: "100%",
  },
  progressTrack: {
    backgroundColor: "#EEF2E9",
    borderRadius: 999,
    flex: 1,
    height: 9,
    overflow: "hidden",
  },
  screenContainer: {
    gap: 16,
  },
  targetIdentity: {
    alignItems: "center",
    flexDirection: "row",
    flexShrink: 1,
    gap: 10,
    minWidth: 0,
  },
  targetLabel: {
    color: "#1B2430",
    flexShrink: 1,
    fontSize: 16,
    fontWeight: "900",
    lineHeight: 20,
  },
  targetList: {
    gap: 15,
  },
  targetPercent: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "900",
    minWidth: 42,
    textAlign: "right",
  },
  targetProgressLine: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    paddingLeft: 44,
  },
  targetRow: {
    gap: 8,
  },
  targetText: {
    color: colors.accentDark,
    fontSize: 14,
    fontWeight: "900",
    lineHeight: 19,
  },
  targetTopLine: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  targetValue: {
    color: colors.muted,
    flexShrink: 0,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  title: {
    color: "#1B2430",
    fontSize: 34,
    fontWeight: "900",
    lineHeight: 40,
  },
});
