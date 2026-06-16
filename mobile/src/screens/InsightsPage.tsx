import { useMemo, useState } from "react";
import type { ComponentType, ReactNode } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import { MacroDonutRing } from "../components/insights/MacroDonutRing";
import {
  BreakfastCoffeeCupIcon,
  DinnerPlateCutleryIcon,
  LunchServingDomeIcon,
  SnackAppleIcon,
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
  carbs_g?: number;
  fat_g?: number;
  kcal?: number;
  protein_g?: number;
  slot: string;
};

type InsightsPageProps = {
  activeProfileKey: string;
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
  scrollToTopSignal?: number;
  selectedDay: InsightsDaySelection;
  targetTotals?: InsightsTotals;
  totals?: InsightsTotals;
};

type IconComponent = ComponentType<{ size?: number }>;
type MealCompletionState = Record<string, boolean>;
type MacroProgress = {
  carbs: number;
  fat: number;
  protein: number;
};

const MACRO_COLORS = {
  calories: colors.accent,
  carbs: "#F28A12",
  fat: "#F5C400",
  protein: "#EF3B45",
};

const MEAL_DASHBOARD_ORDER = ["breakfast", "lunch", "snack", "dinner"];
const EMPTY_COMPLETION_STATE: MealCompletionState = {};
const FULL_MACRO_PROGRESS: MacroProgress = { carbs: 1, fat: 1, protein: 1 };

export function InsightsPage({
  activeProfileKey,
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
  scrollToTopSignal,
  selectedDay,
  targetTotals,
  totals,
}: InsightsPageProps) {
  const isAverageMode = selectedDay === "average";
  const mealRows = useMemo(
    () => getDashboardMealRows(mealContributions),
    [mealContributions],
  );
  const completionContextKey = useMemo(
    () =>
      buildMealCompletionContextKey({
        activeProfileKey,
        activeProfileMeta,
        activeProfileName,
        mealRows,
        selectedDay,
        totals,
      }),
    [
      activeProfileKey,
      activeProfileMeta,
      activeProfileName,
      mealRows,
      selectedDay,
      totals?.carbs_g,
      totals?.fat_g,
      totals?.kcal,
      totals?.protein_g,
    ],
  );
  const [completionByContext, setCompletionByContext] = useState<
    Record<string, MealCompletionState>
  >({});
  const completionForContext = isAverageMode
    ? EMPTY_COMPLETION_STATE
    : completionByContext[completionContextKey] ?? EMPTY_COMPLETION_STATE;
  const consumedTotals = useMemo(
    () => sumCompletedMealTotals(mealRows, completionForContext),
    [completionForContext, mealRows],
  );
  const displayedTotals = isAverageMode ? totals : consumedTotals;
  const macroPercents = macroEnergyPercents(totals);
  const donutProgress = isAverageMode
    ? FULL_MACRO_PROGRESS
    : macroCompletionRatios(consumedTotals, totals);
  const goalBadgeLabel = getGoalBadgeLabel(activeProfileMeta);

  function toggleMealCompletion(mealKey: string) {
    if (isAverageMode) {
      return;
    }
    setCompletionByContext((current) => {
      const contextState = { ...(current[completionContextKey] ?? {}) };
      if (contextState[mealKey]) {
        delete contextState[mealKey];
      } else {
        contextState[mealKey] = true;
      }
      return {
        ...current,
        [completionContextKey]: contextState,
      };
    });
  }

  if (!isSetupComplete) {
    return (
      <AppScreen scrollToTopSignal={scrollToTopSignal}>
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
      <AppScreen scrollToTopSignal={scrollToTopSignal}>
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
      <AppScreen scrollToTopSignal={scrollToTopSignal}>
        <EmptyState
          actionLabel="Go to Meal Plan"
          onAction={onGoToMealPlan}
          text="Generate a meal plan first to see calorie, macro and meal contribution insights."
          title="No insights yet"
        />
      </AppScreen>
    );
  }

  return (
    <AppScreen
      contentContainerStyle={styles.screenContainer}
      scrollToTopSignal={scrollToTopSignal}
    >
      <View style={styles.header}>
        {profileSelector ? (
          <View style={styles.profileSelectorSlot}>{profileSelector}</View>
        ) : (
          <View style={styles.profileFallback}>
            <Text numberOfLines={1} style={styles.profileFallbackName}>
              {activeProfileName}
            </Text>
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
          {goalBadgeLabel ? (
            <Text numberOfLines={1} style={styles.goalBadge}>
              {goalBadgeLabel}
            </Text>
          ) : null}
        </View>
        <View style={styles.balanceContent}>
          <MacroDonutRing
            carbsPercent={macroPercents.carbs}
            consumedCarbsRatio={donutProgress.carbs}
            consumedFatRatio={donutProgress.fat}
            consumedProteinRatio={donutProgress.protein}
            fatPercent={macroPercents.fat}
            proteinPercent={macroPercents.protein}
            size={132}
          />
          <View style={styles.balanceSummary}>
            <Text numberOfLines={1} style={styles.kcalValue}>
              {isAverageMode
                ? `${formatNumber(totals?.kcal)} kcal`
                : `${formatNumber(displayedTotals?.kcal)}/${formatNumber(totals?.kcal)} kcal`}
            </Text>
            <Text style={styles.targetText}>
              {isAverageMode ? "Daily total" : "Consumed so far"}
            </Text>
            <View style={styles.legendList}>
              <MacroLegendRow
                actual={displayedTotals?.protein_g}
                color={MACRO_COLORS.protein}
                label="Protein"
                planned={totals?.protein_g}
                showProgress={!isAverageMode}
              />
              <MacroLegendRow
                actual={displayedTotals?.carbs_g}
                color={MACRO_COLORS.carbs}
                label="Carbs"
                planned={totals?.carbs_g}
                showProgress={!isAverageMode}
              />
              <MacroLegendRow
                actual={displayedTotals?.fat_g}
                color={MACRO_COLORS.fat}
                label="Fats"
                planned={totals?.fat_g}
                showProgress={!isAverageMode}
              />
            </View>
          </View>
        </View>
      </View>

      <View style={[styles.dashboardCard, styles.mealContributionCard]}>
        <Text style={styles.mealContributionTitle}>Meal contribution</Text>
        {mealRows.length ? (
          <View style={styles.mealContributionGrid}>
            {mealRows.map((meal) => {
              const mealKey = getMealContributionKey(meal);
              return (
                <MealContributionItem
                  isEaten={Boolean(completionForContext[mealKey])}
                  isInteractive={!isAverageMode}
                  key={mealKey}
                  meal={meal}
                  onToggle={() => toggleMealCompletion(mealKey)}
                  plannedDayKcal={totals?.kcal}
                />
              );
            })}
          </View>
        ) : (
          <Text style={styles.emptyDashboardText}>
            No meal breakdown available for this day.
          </Text>
        )}
      </View>

      <View style={styles.dashboardCard}>
        <Text style={styles.cardTitle}>Macro Targets</Text>
        <View style={styles.targetList}>
          <MacroTargetRow
            Icon={CaloriesFlameIcon}
            actual={displayedTotals?.kcal}
            color={MACRO_COLORS.calories}
            label="Calories"
            target={targetTotals?.kcal}
            unit="kcal"
          />
          <MacroTargetRow
            Icon={ProteinDrumstickIcon}
            actual={displayedTotals?.protein_g}
            color={MACRO_COLORS.protein}
            label="Protein"
            target={targetTotals?.protein_g}
            unit="g"
          />
          <MacroTargetRow
            Icon={CarbsWheatIcon}
            actual={displayedTotals?.carbs_g}
            color={MACRO_COLORS.carbs}
            label="Carbs"
            target={targetTotals?.carbs_g}
            unit="g"
          />
          <MacroTargetRow
            Icon={FatsAvocadoIcon}
            actual={displayedTotals?.fat_g}
            color={MACRO_COLORS.fat}
            label="Fats"
            target={targetTotals?.fat_g}
            unit="g"
          />
        </View>
      </View>

      <View style={styles.dashboardCard}>
        <View style={styles.cardHeader}>
          <Text style={styles.cardTitle}>Micronutrients</Text>
        </View>
        <Text style={styles.bodyText}>
          Limited micronutrient data available.
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
  const dayOptions = [1, 2, 3, 4, 5];
  const averageEnabled = dayIndexes.length > 0;
  const averageActive = selected === "average";

  return (
    <View style={styles.daySelector}>
      <View style={styles.daySelectorRow}>
        {dayOptions.map((option) => {
          const enabled = generatedDays.has(option);
          const active = selected === option;
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
                {`Day ${option}`}
              </Text>
            </Pressable>
          );
        })}
      </View>
      <Pressable
        accessibilityRole="button"
        disabled={!averageEnabled}
        onPress={() => onSelect("average")}
        style={({ pressed }) => [
          styles.averageChip,
          averageActive ? styles.dayChipActive : null,
          !averageActive && averageEnabled ? styles.dayChipEnabled : null,
          !averageEnabled ? styles.dayChipDisabled : null,
          pressed && averageEnabled ? styles.pressed : null,
        ]}
      >
        <Text
          numberOfLines={1}
          style={[
            styles.dayChipText,
            averageActive ? styles.dayChipTextActive : null,
            !averageEnabled ? styles.dayChipTextDisabled : null,
          ]}
        >
          Average
        </Text>
      </Pressable>
    </View>
  );
}

function MacroLegendRow({
  actual,
  color,
  label,
  planned,
  showProgress,
}: {
  actual?: number;
  color: string;
  label: string;
  planned?: number;
  showProgress: boolean;
}) {
  return (
    <View style={styles.legendRow}>
      <View style={[styles.legendDot, { backgroundColor: color }]} />
      <Text numberOfLines={1} style={styles.legendLabel}>
        {label}
      </Text>
      <Text numberOfLines={1} style={styles.legendProgressValue}>
        {showProgress
          ? `${formatNumber(actual)} / ${formatNumber(planned)} g`
          : `${formatNumber(actual)} g`}
      </Text>
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
  const percentText = percent === null ? "-" : `${Math.round(percent)}%`;

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
          {formatNumber(actual)} / {target == null ? "-" : formatNumber(target)} {unit} {"\u00B7"} {percentText}
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
      </View>
    </View>
  );
}

function MealContributionItem({
  isEaten,
  isInteractive,
  meal,
  onToggle,
  plannedDayKcal,
}: {
  isEaten: boolean;
  isInteractive: boolean;
  meal: MealContribution;
  onToggle: () => void;
  plannedDayKcal?: number;
}) {
  const normalizedSlot = normalizeSlot(meal.slot);
  const Icon = getMealIcon(normalizedSlot);
  const mealName = formatMealSlot(normalizedSlot);
  const kcalPercent = percentOfTarget(meal.kcal, plannedDayKcal);

  return (
    <Pressable
      accessibilityRole={isInteractive ? "button" : undefined}
      disabled={!isInteractive}
      onPress={onToggle}
      style={({ pressed }) => [
        styles.mealContributionItem,
        isEaten ? styles.mealContributionItemEaten : null,
        pressed && isInteractive ? styles.mealContributionItemPressed : null,
      ]}
    >
      <View
        style={[
          styles.mealIconShell,
          isEaten ? styles.mealIconShellEaten : null,
        ]}
      >
        <Icon size={26} />
        {isEaten ? (
          <View style={styles.mealCheckBadge}>
            <Text style={styles.mealCheckText}>✓</Text>
          </View>
        ) : null}
      </View>
      <Text numberOfLines={1} style={styles.mealSlot}>
        {mealName}
      </Text>
      <Text numberOfLines={1} style={styles.mealKcalValue}>
        {formatNumber(meal.kcal)} kcal
      </Text>
      <Text numberOfLines={1} style={styles.mealProteinValue}>
        P {formatNumber(meal.protein_g)} g {"\u00B7"} {formatOptionalPercent(kcalPercent)}
      </Text>
    </Pressable>
  );
}

function getMealIcon(slot: string): IconComponent {
  if (slot === "breakfast") {
    return BreakfastCoffeeCupIcon;
  }
  if (slot === "snack") {
    return SnackAppleIcon;
  }
  if (slot === "dinner") {
    return DinnerPlateCutleryIcon;
  }
  return LunchServingDomeIcon;
}

function getDashboardMealRows(meals: MealContribution[]): MealContribution[] {
  const bySlot = new Map<string, MealContribution>();
  for (const meal of meals) {
    bySlot.set(normalizeSlot(meal.slot), meal);
  }
  const dashboardRows = MEAL_DASHBOARD_ORDER
    .map((slot) => bySlot.get(slot))
    .filter((meal): meal is MealContribution => Boolean(meal));
  const orderedSlots = new Set(MEAL_DASHBOARD_ORDER);
  const extraRows = meals.filter((meal) => !orderedSlots.has(normalizeSlot(meal.slot)));
  return [...dashboardRows, ...extraRows];
}

function buildMealCompletionContextKey({
  activeProfileKey,
  activeProfileMeta,
  activeProfileName,
  mealRows,
  selectedDay,
  totals,
}: {
  activeProfileKey: string;
  activeProfileMeta: string;
  activeProfileName: string;
  mealRows: MealContribution[];
  selectedDay: InsightsDaySelection;
  totals?: InsightsTotals;
}) {
  const profileKey = activeProfileKey || `${activeProfileName}:${activeProfileMeta}`;
  const totalsSignature = [
    totals?.kcal,
    totals?.protein_g,
    totals?.carbs_g,
    totals?.fat_g,
  ]
    .map(formatSignatureNumber)
    .join(",");
  const mealSignature = mealRows
    .map((meal) => {
      const metricSignature = [
        meal.kcal,
        meal.protein_g,
        meal.carbs_g,
        meal.fat_g,
      ]
        .map(formatSignatureNumber)
        .join(":");
      return `${getMealContributionKey(meal)}:${metricSignature}`;
    })
    .join("|");
  return [profileKey, String(selectedDay), totalsSignature, mealSignature].join("::");
}

function getMealContributionKey(meal: MealContribution): string {
  return normalizeSlot(meal.slot || "meal");
}

function sumCompletedMealTotals(
  mealRows: MealContribution[],
  completionState: MealCompletionState,
): InsightsTotals {
  return mealRows.reduce<Required<InsightsTotals>>(
    (totals, meal) => {
      if (!completionState[getMealContributionKey(meal)]) {
        return totals;
      }
      return {
        carbs_g: totals.carbs_g + numberValue(meal.carbs_g),
        fat_g: totals.fat_g + numberValue(meal.fat_g),
        kcal: totals.kcal + numberValue(meal.kcal),
        protein_g: totals.protein_g + numberValue(meal.protein_g),
      };
    },
    { carbs_g: 0, fat_g: 0, kcal: 0, protein_g: 0 },
  );
}

function macroCompletionRatios(consumed: InsightsTotals, planned?: InsightsTotals): MacroProgress {
  return {
    carbs: ratioOf(consumed.carbs_g, planned?.carbs_g),
    fat: ratioOf(consumed.fat_g, planned?.fat_g),
    protein: ratioOf(consumed.protein_g, planned?.protein_g),
  };
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

function ratioOf(actual?: number, target?: number): number {
  const parsedTarget = numberValue(target);
  if (parsedTarget <= 0) {
    return 0;
  }
  return Math.min(1, Math.max(0, numberValue(actual) / parsedTarget));
}

function getGoalBadgeLabel(meta?: string): string | null {
  if (!meta) {
    return null;
  }
  const goal = meta.replace(/^Goal:\s*/i, "").split(/\s+-\s+|\s+\u00B7\s+/)[0]?.trim();
  return goal || null;
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

function formatOptionalPercent(value: number | null): string {
  return value === null ? "-" : formatPercent(value);
}

function formatSignatureNumber(value: unknown): string {
  return String(Math.round(numberValue(value) * 10) / 10);
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
  averageChip: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 38,
    minWidth: 0,
    paddingHorizontal: 4,
    paddingVertical: 8,
    width: "100%",
  },
  balanceContent: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
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
    fontSize: 21,
    fontWeight: "900",
    lineHeight: 26,
  },
  cardSubheading: {
    color: "#1B2430",
    fontSize: 14,
    fontWeight: "900",
    lineHeight: 18,
  },
  dashboardCard: {
    backgroundColor: colors.card,
    borderColor: "#E0E7DA",
    borderRadius: 24,
    borderWidth: 1,
    elevation: 2,
    gap: 15,
    paddingHorizontal: 18,
    paddingVertical: 18,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 7 },
    shadowOpacity: 0.08,
    shadowRadius: 14,
  },
  dayChip: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 38,
    minWidth: 0,
    paddingHorizontal: 4,
    paddingVertical: 8,
  },
  dayChipActive: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  dayChipDisabled: {
    backgroundColor: "#F3F4F6",
    borderColor: "#D1D5DB",
  },
  dayChipEnabled: {
    backgroundColor: colors.card,
    borderColor: colors.accent,
  },
  dayChipText: {
    color: colors.accent,
    fontSize: 12,
    fontWeight: "800",
    textAlign: "center",
  },
  dayChipTextActive: {
    color: "#FFFFFF",
  },
  dayChipTextDisabled: {
    color: "#9CA3AF",
  },
  daySelector: {
    gap: 8,
  },
  daySelectorRow: {
    flexDirection: "row",
    gap: 6,
    width: "100%",
  },
  emptyDashboardText: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 20,
  },
  header: {
    alignItems: "center",
    gap: 6,
    paddingTop: 2,
  },
  goalBadge: {
    backgroundColor: "#F1F8EA",
    borderColor: "#DDEFCF",
    borderRadius: 999,
    borderWidth: 1,
    color: colors.accentDark,
    flexShrink: 0,
    fontSize: 13,
    fontWeight: "900",
    maxWidth: "48%",
    overflow: "hidden",
    paddingHorizontal: 10,
    paddingVertical: 5,
    textAlign: "center",
  },
  kcalValue: {
    color: "#1B2430",
    fontSize: 18,
    fontWeight: "900",
    lineHeight: 23,
  },
  legendDot: {
    borderRadius: 999,
    height: 9,
    width: 9,
  },
  legendLabel: {
    color: colors.muted,
    flexShrink: 0,
    fontSize: 13,
    fontWeight: "800",
    minWidth: 58,
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
  legendProgressValue: {
    color: "#1B2430",
    flex: 1,
    fontSize: 11,
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
    height: 50,
    justifyContent: "center",
    position: "relative",
    width: 50,
  },
  mealIconShellEaten: {
    backgroundColor: "#FFFFFF",
  },
  mealCheckBadge: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderColor: "#FFFFFF",
    borderRadius: 999,
    borderWidth: 1.5,
    bottom: -2,
    height: 14,
    justifyContent: "center",
    position: "absolute",
    right: -2,
    width: 14,
  },
  mealCheckText: {
    color: "#FFFFFF",
    fontSize: 8,
    fontWeight: "900",
    lineHeight: 10,
  },
  mealContributionCard: {
    gap: 13,
    paddingVertical: 16,
  },
  mealContributionGrid: {
    alignItems: "stretch",
    flexDirection: "row",
    gap: 6,
  },
  mealContributionItem: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: "#E2ECD8",
    borderRadius: 16,
    borderWidth: 1,
    flex: 1,
    flexBasis: 0,
    gap: 4,
    justifyContent: "flex-start",
    minHeight: 124,
    minWidth: 0,
    paddingHorizontal: 2,
    paddingVertical: 8,
  },
  mealContributionItemEaten: {
    backgroundColor: "#F4FAEE",
    borderColor: "#DDEFCF",
  },
  mealContributionItemPressed: {
    opacity: 0.82,
  },
  mealContributionTitle: {
    color: "#1B2430",
    fontSize: 17,
    fontWeight: "900",
    lineHeight: 22,
  },
  mealSlot: {
    color: "#1B2430",
    fontSize: 11,
    fontWeight: "900",
    lineHeight: 15,
    marginTop: 2,
    textAlign: "center",
  },
  mealKcalValue: {
    color: "#1B2430",
    fontSize: 10,
    fontWeight: "800",
    lineHeight: 14,
    textAlign: "center",
  },
  mealProteinValue: {
    color: colors.muted,
    fontSize: 9,
    fontWeight: "700",
    lineHeight: 14,
    textAlign: "center",
  },
  mealPercentValue: {
    color: colors.accentDark,
    fontSize: 10,
    fontWeight: "800",
    lineHeight: 14,
    textAlign: "center",
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
  profileFallbackName: {
    color: "#1B2430",
    fontSize: 17,
    fontWeight: "900",
    lineHeight: 21,
  },
  profileSelectorSlot: {
    alignSelf: "stretch",
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
    gap: 14,
    paddingTop: 14,
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
    gap: 12,
  },
  targetProgressLine: {
    alignItems: "center",
    flexDirection: "row",
    paddingLeft: 44,
  },
  targetRow: {
    gap: 6,
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
    flexShrink: 1,
    fontSize: 13,
    fontWeight: "800",
    textAlign: "right",
  },
});
