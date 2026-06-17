import { useEffect, useMemo, useState } from "react";
import type { ComponentType, ReactNode } from "react";
import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";

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
import type { DailyProgressSnapshot } from "../types/api";

export type InsightsDaySelection = number | "average";

export type InsightsTotals = {
  carbs_g?: number;
  fat_g?: number;
  kcal?: number;
  protein_g?: number;
};

export type MealContribution = {
  carbs_g?: number;
  display_name?: string;
  fat_g?: number;
  kcal?: number;
  protein_g?: number;
  recipe_id?: string;
  slot: string;
};

export type DailyProgressSaveInput = {
  consumed: InsightsTotals;
  day_snapshot: Record<string, unknown>;
  meal_completion: Record<string, unknown>;
  planned: InsightsTotals;
  target: InsightsTotals;
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
  onDeleteProgress?: (progressId: string) => void;
  onSaveProgress?: (input: DailyProgressSaveInput) => void;
  onSelectDay: (value: InsightsDaySelection) => void;
  profileSelector?: ReactNode;
  progressError?: string;
  progressHistory?: DailyProgressSnapshot[];
  progressUnavailableReason?: string;
  scrollToTopSignal?: number;
  selectedDay: InsightsDaySelection;
  savedProgress?: DailyProgressSnapshot | null;
  isDeletingProgress?: boolean;
  isLoadingProgress?: boolean;
  isSavingProgress?: boolean;
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
type TrendMetricKey = "calories" | "protein" | "carbs" | "fats";
type TrendStatus = "in_target" | "close" | "off";

const MACRO_COLORS = {
  calories: colors.accent,
  carbs: "#F28A12",
  fat: "#F5C400",
  protein: "#EF3B45",
};

const MEAL_DASHBOARD_ORDER = ["breakfast", "lunch", "snack", "dinner"];
const EMPTY_COMPLETION_STATE: MealCompletionState = {};
const FULL_MACRO_PROGRESS: MacroProgress = { carbs: 1, fat: 1, protein: 1 };
const TREND_BLOCK_SIZE = 5;
const TREND_METRICS: Array<{ key: TrendMetricKey; label: string; unit: string }> = [
  { key: "calories", label: "Calories", unit: "kcal" },
  { key: "protein", label: "Protein", unit: "g" },
  { key: "carbs", label: "Carbs", unit: "g" },
  { key: "fats", label: "Fats", unit: "g" },
];
const TREND_STATUS_COLORS: Record<TrendStatus, string> = {
  close: "#D7A72B",
  in_target: "#86B955",
  off: "#D8786C",
};
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
  onDeleteProgress,
  onSaveProgress,
  onSelectDay,
  profileSelector,
  progressError,
  progressHistory,
  progressUnavailableReason,
  scrollToTopSignal,
  selectedDay,
  savedProgress,
  isDeletingProgress,
  isLoadingProgress,
  isSavingProgress,
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

  function saveCurrentDayProgress() {
    if (!onSaveProgress || isAverageMode) {
      return;
    }
    onSaveProgress(
      buildDailyProgressSaveInput({
        activeProfileKey,
        activeProfileMeta,
        activeProfileName,
        completionState: completionForContext,
        consumedTotals,
        mealRows,
        plannedTotals: totals,
        selectedDay,
        targetTotals,
      }),
    );
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

      <DailyProgressControl
        disabledReason={
          isAverageMode
            ? "Average mode cannot be saved as a day."
            : progressUnavailableReason
        }
        isDeleting={Boolean(isDeletingProgress)}
        isLoading={Boolean(isLoadingProgress)}
        isSaving={Boolean(isSavingProgress)}
        onDelete={
          savedProgress && onDeleteProgress
            ? () => onDeleteProgress(savedProgress.progress_id)
            : undefined
        }
        onSave={saveCurrentDayProgress}
        progressError={progressError}
        savedProgress={savedProgress ?? null}
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

      <TrendsSection snapshots={progressHistory ?? []} />

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

function DailyProgressControl({
  disabledReason,
  isDeleting,
  isLoading,
  isSaving,
  onDelete,
  onSave,
  progressError,
  savedProgress,
}: {
  disabledReason?: string;
  isDeleting: boolean;
  isLoading: boolean;
  isSaving: boolean;
  onDelete?: () => void;
  onSave: () => void;
  progressError?: string;
  savedProgress: DailyProgressSnapshot | null;
}) {
  const hasSavedProgress = Boolean(savedProgress);
  const statusText = isLoading
    ? "Checking saved progress..."
    : hasSavedProgress
      ? "Saved to progress history"
      : "Progress not saved yet";
  const canSave = !disabledReason && !hasSavedProgress && !isLoading && !isSaving;

  return (
    <View style={styles.progressControlCard}>
      <View style={styles.progressStatusRow}>
        <View style={styles.progressStatusTextBlock}>
          <Text style={styles.progressStatusLabel}>Daily progress</Text>
          <Text style={styles.progressStatusText}>{statusText}</Text>
          {progressError ? (
            <Text style={styles.progressErrorText}>{progressError}</Text>
          ) : null}
        </View>
        {isLoading ? <ActivityIndicator color={colors.accent} size="small" /> : null}
      </View>
      <View style={styles.progressActionRow}>
        <Pressable
          accessibilityRole="button"
          disabled={!canSave}
          onPress={onSave}
          style={({ pressed }) => [
            styles.progressPrimaryButton,
            hasSavedProgress ? styles.progressSavedButtonDisabled : null,
            !canSave && !hasSavedProgress ? styles.progressButtonDisabled : null,
            pressed && canSave ? styles.pressed : null,
          ]}
        >
          <Text
            style={[
              styles.progressPrimaryText,
              hasSavedProgress ? styles.progressSavedButtonText : null,
              !canSave && !hasSavedProgress ? styles.progressButtonTextDisabled : null,
            ]}
          >
            {hasSavedProgress ? "Saved" : isSaving ? "Saving..." : "Save day"}
          </Text>
        </Pressable>
        {hasSavedProgress && onDelete ? (
          <Pressable
            accessibilityRole="button"
            disabled={isDeleting}
            onPress={onDelete}
            style={({ pressed }) => [
              styles.progressSecondaryButton,
              isDeleting ? styles.progressButtonDisabled : null,
              pressed && !isDeleting ? styles.pressed : null,
            ]}
          >
            <Text
              style={[
                styles.progressSecondaryText,
                isDeleting ? styles.progressButtonTextDisabled : null,
              ]}
            >
              {isDeleting ? "Deleting..." : "Delete saved day"}
            </Text>
          </Pressable>
        ) : null}
      </View>
    </View>
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

function TrendsSection({ snapshots }: { snapshots: DailyProgressSnapshot[] }) {
  const orderedSnapshots = useMemo(() => normalizeTrendSnapshots(snapshots), [snapshots]);
  const maxRange = Math.min(30, orderedSnapshots.length);
  const [selectedRange, setSelectedRange] = useState(maxRange);
  const [selectedMetric, setSelectedMetric] = useState<TrendMetricKey>("calories");
  const [blockIndex, setBlockIndex] = useState(0);
  const historySignature = orderedSnapshots
    .map((snapshot) => snapshot.progress_id)
    .join("|");

  useEffect(() => {
    setSelectedRange(maxRange);
    setBlockIndex(0);
  }, [historySignature, maxRange]);

  const safeRange = maxRange > 0 ? Math.min(Math.max(selectedRange || maxRange, 1), maxRange) : 0;
  const visibleSnapshots = safeRange > 0 ? orderedSnapshots.slice(-safeRange) : [];
  const blockCount = Math.max(1, Math.ceil(visibleSnapshots.length / TREND_BLOCK_SIZE));

  useEffect(() => {
    setBlockIndex((current) => Math.min(current, blockCount - 1));
  }, [blockCount]);

  if (!orderedSnapshots.length) {
    return (
      <View style={[styles.dashboardCard, styles.trendsEmptyCard]}>
        <Text style={styles.cardTitle}>Trends</Text>
        <View style={styles.trendsEmptyState}>
          <Text style={styles.trendsEmptyTitle}>No saved progress yet</Text>
          <Text style={styles.trendsEmptyText}>
            Save a day from Insights to start seeing trends.
          </Text>
        </View>
      </View>
    );
  }

  return (
    <View style={styles.trendsSection}>
      <TargetAdherenceCard
        metric={selectedMetric}
        onSelectMetric={setSelectedMetric}
        rangeSelector={
          <RangeSelector
            maxRange={maxRange}
            onChange={(nextRange) => {
              setSelectedRange(nextRange);
              setBlockIndex(0);
            }}
            value={safeRange}
          />
        }
        snapshots={visibleSnapshots}
      />
      <ConsistencyCard snapshots={visibleSnapshots} />
      <MacroPatternHeatmap
        blockIndex={blockIndex}
        onChangeBlock={setBlockIndex}
        snapshots={visibleSnapshots}
      />
    </View>
  );
}

function RangeSelector({
  maxRange,
  onChange,
  value,
}: {
  maxRange: number;
  onChange: (value: number) => void;
  value: number;
}) {
  const canDecrease = value > 1;
  const canIncrease = value < maxRange;
  return (
    <View style={styles.rangeSelector}>
      <Pressable
        accessibilityRole="button"
        disabled={!canDecrease}
        onPress={() => onChange(Math.max(1, value - 1))}
        style={({ pressed }) => [
          styles.rangeButton,
          !canDecrease ? styles.rangeButtonDisabled : null,
          pressed && canDecrease ? styles.pressed : null,
        ]}
      >
        <Text style={[styles.rangeButtonText, !canDecrease ? styles.rangeButtonTextDisabled : null]}>
          -
        </Text>
      </Pressable>
      <Text numberOfLines={1} style={styles.rangeLabel}>
        Last {value} {value === 1 ? "day" : "days"}
      </Text>
      <Pressable
        accessibilityRole="button"
        disabled={!canIncrease}
        onPress={() => onChange(Math.min(maxRange, value + 1))}
        style={({ pressed }) => [
          styles.rangeButton,
          !canIncrease ? styles.rangeButtonDisabled : null,
          pressed && canIncrease ? styles.pressed : null,
        ]}
      >
        <Text style={[styles.rangeButtonText, !canIncrease ? styles.rangeButtonTextDisabled : null]}>
          +
        </Text>
      </Pressable>
    </View>
  );
}

function MetricSelector({
  onSelect,
  selected,
}: {
  onSelect: (metric: TrendMetricKey) => void;
  selected: TrendMetricKey;
}) {
  return (
    <View style={styles.metricSelector}>
      {TREND_METRICS.map((metric) => {
        const active = metric.key === selected;
        return (
          <Pressable
            accessibilityRole="button"
            key={metric.key}
            onPress={() => onSelect(metric.key)}
            style={({ pressed }) => [
              styles.metricChip,
              active ? styles.metricChipActive : null,
              pressed ? styles.pressed : null,
            ]}
          >
            <Text
              numberOfLines={1}
              style={[styles.metricChipText, active ? styles.metricChipTextActive : null]}
            >
              {metric.label}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

function TargetAdherenceCard({
  metric,
  onSelectMetric,
  rangeSelector,
  snapshots,
}: {
  metric: TrendMetricKey;
  onSelectMetric: (metric: TrendMetricKey) => void;
  rangeSelector: ReactNode;
  snapshots: DailyProgressSnapshot[];
}) {
  const metricConfig = TREND_METRICS.find((item) => item.key === metric) ?? TREND_METRICS[0];
  const points = snapshots.map((snapshot, index) => {
    const actual = getTrendActual(snapshot, metric);
    const target = getTrendTarget(snapshot, metric);
    return {
      actual,
      index,
      status: getTrendStatus(metric, actual, target),
      target,
    };
  });
  const maxValue = Math.max(
    1,
    ...points.flatMap((point) => [point.actual, point.target]).map(numberValue),
  );

  return (
    <View style={[styles.dashboardCard, styles.trendCard]}>
      <View style={styles.trendCardHeader}>
        <Text style={styles.trendCardTitle}>Target adherence</Text>
        {rangeSelector}
      </View>
      <MetricSelector onSelect={onSelectMetric} selected={metric} />
      <View style={styles.trendChart}>
        {points.map((point) => {
          const actualPercent = Math.min(100, Math.max(4, (point.actual / maxValue) * 100));
          const targetPercent = Math.min(100, Math.max(4, (point.target / maxValue) * 100));
          return (
            <View key={`${metric}-${point.index}`} style={styles.trendBarColumn}>
              <View style={styles.trendBarTrack}>
                <View
                  style={[
                    styles.trendTargetMarker,
                    { bottom: `${targetPercent}%` as `${number}%` },
                  ]}
                />
                <View
                  style={[
                    styles.trendBarFill,
                    {
                      backgroundColor: TREND_STATUS_COLORS[point.status],
                      height: `${actualPercent}%` as `${number}%`,
                    },
                  ]}
                />
              </View>
            </View>
          );
        })}
      </View>
      <View style={styles.trendChartFooter}>
        <Text style={styles.trendTinyText}>Oldest</Text>
        <Text style={styles.trendTinyText}>
          Target: {formatTrendValue(latestTrendTarget(points), metricConfig.unit)}
        </Text>
        <Text style={styles.trendTinyText}>Newest</Text>
      </View>
      <View style={styles.trendLegend}>
        <TrendLegendItem align="left" label="In target" status="in_target" />
        <TrendLegendItem align="center" label="Close" status="close" />
        <TrendLegendItem align="right" label="Off target" status="off" />
      </View>
    </View>
  );
}

function TrendLegendItem({
  align,
  label,
  status,
}: {
  align: "center" | "left" | "right";
  label: string;
  status: TrendStatus;
}) {
  return (
    <View
      style={[
        styles.trendLegendItem,
        align === "center" ? styles.trendLegendItemCenter : null,
        align === "right" ? styles.trendLegendItemRight : null,
      ]}
    >
      <View style={[styles.trendLegendDot, { backgroundColor: TREND_STATUS_COLORS[status] }]} />
      <Text style={styles.trendTinyText}>{label}</Text>
    </View>
  );
}

function ConsistencyCard({ snapshots }: { snapshots: DailyProgressSnapshot[] }) {
  const dayStatuses = snapshots.map(isSnapshotInConsistencyRange);
  const daysInRange = dayStatuses.filter(Boolean).length;
  const totalDays = snapshots.length;
  const consistencyPercent = totalDays > 0 ? Math.round((daysInRange / totalDays) * 100) : 0;
  const currentStreak = countCurrentConsistencyStreak(dayStatuses);
  const progressWidth = `${Math.min(100, Math.max(0, consistencyPercent))}%` as `${number}%`;

  return (
    <View style={[styles.dashboardCard, styles.trendCard]}>
      <Text style={styles.cardSubheading}>Consistency</Text>
      <View style={styles.consistencyMetrics}>
        <View style={styles.consistencyMetricBlock}>
          <Text style={styles.consistencyValue}>
            {daysInRange} / {totalDays}
          </Text>
          <Text style={styles.consistencyLabel}>days in range</Text>
        </View>
        <View style={styles.consistencyMetricBlock}>
          <Text style={styles.consistencyValue}>{consistencyPercent}%</Text>
          <Text style={styles.consistencyLabel}>consistency</Text>
        </View>
        <View style={styles.consistencyMetricBlock}>
          <Text style={styles.consistencyValue}>
            {currentStreak} {currentStreak === 1 ? "day" : "days"}
          </Text>
          <Text style={styles.consistencyLabel}>Current streak</Text>
        </View>
      </View>
      <View style={styles.consistencyProgressTrack}>
        <View style={[styles.consistencyProgressFill, { width: progressWidth }]} />
      </View>
      <Text style={styles.trendHelperText}>Based on calories and protein targets.</Text>
    </View>
  );
}

function MacroPatternHeatmap({
  blockIndex,
  onChangeBlock,
  snapshots,
}: {
  blockIndex: number;
  onChangeBlock: (index: number) => void;
  snapshots: DailyProgressSnapshot[];
}) {
  const blockCount = Math.max(1, Math.ceil(snapshots.length / TREND_BLOCK_SIZE));
  const safeBlockIndex = Math.min(blockIndex, blockCount - 1);
  const blockStart = safeBlockIndex * TREND_BLOCK_SIZE;
  const blockSnapshots = snapshots.slice(blockStart, blockStart + TREND_BLOCK_SIZE);
  const blockEnd = blockStart + blockSnapshots.length;
  const canGoBack = safeBlockIndex > 0;
  const canGoForward = safeBlockIndex < blockCount - 1;

  return (
    <View style={[styles.dashboardCard, styles.trendCard]}>
      <View style={styles.patternHeader}>
        <Text style={styles.cardSubheading}>Macro pattern</Text>
        <View style={styles.patternNav}>
          <Pressable
            accessibilityRole="button"
            disabled={!canGoBack}
            onPress={() => onChangeBlock(Math.max(0, safeBlockIndex - 1))}
            style={({ pressed }) => [
              styles.patternArrowButton,
              !canGoBack ? styles.patternArrowButtonDisabled : null,
              pressed && canGoBack ? styles.pressed : null,
            ]}
          >
            <Text
              style={[
                styles.patternArrowText,
                !canGoBack ? styles.patternArrowTextDisabled : null,
              ]}
            >
              {"<"}
            </Text>
          </Pressable>
          <Text numberOfLines={1} style={styles.patternRangeText}>
            Days {blockStart + 1}-{blockEnd}
          </Text>
          <Pressable
            accessibilityRole="button"
            disabled={!canGoForward}
            onPress={() => onChangeBlock(Math.min(blockCount - 1, safeBlockIndex + 1))}
            style={({ pressed }) => [
              styles.patternArrowButton,
              !canGoForward ? styles.patternArrowButtonDisabled : null,
              pressed && canGoForward ? styles.pressed : null,
            ]}
          >
            <Text
              style={[
                styles.patternArrowText,
                !canGoForward ? styles.patternArrowTextDisabled : null,
              ]}
            >
              {">"}
            </Text>
          </Pressable>
        </View>
      </View>
      <View style={styles.patternRows}>
        <View style={styles.patternRow}>
          <Text numberOfLines={1} style={[styles.patternMetricLabel, styles.patternDayLabel]}>
            Day
          </Text>
          <View style={styles.patternCells}>
            {blockSnapshots.map((snapshot, index) => (
              <View
                key={`day-${snapshot.progress_id}`}
                style={[styles.patternCell, styles.patternDayCell]}
              >
                <Text style={styles.patternDayText}>{blockStart + index + 1}</Text>
              </View>
            ))}
          </View>
        </View>
        {TREND_METRICS.map((metric) => (
          <View key={metric.key} style={styles.patternRow}>
            <Text numberOfLines={1} style={styles.patternMetricLabel}>
              {metric.label}
            </Text>
            <View style={styles.patternCells}>
              {blockSnapshots.map((snapshot) => {
                const actual = getTrendActual(snapshot, metric.key);
                const target = getTrendTarget(snapshot, metric.key);
                const status = getTrendStatus(metric.key, actual, target);
                return (
                  <View
                    key={`${metric.key}-${snapshot.progress_id}`}
                    style={[
                      styles.patternCell,
                      {
                        backgroundColor: TREND_STATUS_COLORS[status],
                        borderColor: TREND_STATUS_COLORS[status],
                      },
                    ]}
                  />
                );
              })}
            </View>
          </View>
        ))}
      </View>
    </View>
  );
}

function normalizeTrendSnapshots(snapshots: DailyProgressSnapshot[]): DailyProgressSnapshot[] {
  return [...snapshots]
    .sort(compareTrendSnapshotsAscending)
    .slice(-30);
}

function compareTrendSnapshotsAscending(
  left: DailyProgressSnapshot,
  right: DailyProgressSnapshot,
): number {
  const leftTime = Date.parse(left.saved_at || left.created_at || "");
  const rightTime = Date.parse(right.saved_at || right.created_at || "");
  if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) {
    return leftTime - rightTime;
  }
  if (left.plan_id !== right.plan_id) {
    return left.plan_id.localeCompare(right.plan_id);
  }
  return left.day_index - right.day_index;
}

function getTrendActual(snapshot: DailyProgressSnapshot, metric: TrendMetricKey): number {
  if (metric === "protein") {
    return numberValue(snapshot.consumed?.protein_g);
  }
  if (metric === "carbs") {
    return numberValue(snapshot.consumed?.carbs_g);
  }
  if (metric === "fats") {
    return numberValue(snapshot.consumed?.fat_g);
  }
  return numberValue(snapshot.consumed?.kcal);
}

function getTrendTarget(snapshot: DailyProgressSnapshot, metric: TrendMetricKey): number {
  const target = snapshot.target ?? snapshot.planned;
  const planned = snapshot.planned;
  if (metric === "protein") {
    return numberValue(target?.protein_g || planned?.protein_g);
  }
  if (metric === "carbs") {
    return numberValue(target?.carbs_g || planned?.carbs_g);
  }
  if (metric === "fats") {
    return numberValue(target?.fat_g || planned?.fat_g);
  }
  return numberValue(target?.kcal || planned?.kcal);
}

function getTrendStatus(
  metric: TrendMetricKey,
  actual: number,
  target: number,
): TrendStatus {
  if (target <= 0) {
    return "off";
  }
  const ratio = actual / target;
  // Praguri MVP pentru aderenta la target, nu interpretare medicala.
  if (metric === "protein") {
    if (ratio >= 0.9 && ratio <= 1.3) {
      return "in_target";
    }
    if ((ratio >= 0.75 && ratio < 0.9) || (ratio > 1.3 && ratio <= 1.5)) {
      return "close";
    }
    return "off";
  }
  if (ratio >= 0.9 && ratio <= 1.1) {
    return "in_target";
  }
  if ((ratio >= 0.8 && ratio < 0.9) || (ratio > 1.1 && ratio <= 1.2)) {
    return "close";
  }
  return "off";
}

function isSnapshotInConsistencyRange(snapshot: DailyProgressSnapshot): boolean {
  const caloriesStatus = getTrendStatus(
    "calories",
    getTrendActual(snapshot, "calories"),
    getTrendTarget(snapshot, "calories"),
  );
  const proteinStatus = getTrendStatus(
    "protein",
    getTrendActual(snapshot, "protein"),
    getTrendTarget(snapshot, "protein"),
  );
  return caloriesStatus !== "off" && proteinStatus !== "off";
}

function countCurrentConsistencyStreak(dayStatuses: boolean[]): number {
  let streak = 0;
  for (let index = dayStatuses.length - 1; index >= 0; index -= 1) {
    if (!dayStatuses[index]) {
      break;
    }
    streak += 1;
  }
  return streak;
}

function latestTrendTarget(points: Array<{ target: number }>): number {
  return points.length ? points[points.length - 1].target : 0;
}

function formatTrendValue(value: number, unit: string): string {
  return `${formatNumber(value)} ${unit}`;
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

function buildDailyProgressSaveInput({
  activeProfileKey,
  activeProfileMeta,
  activeProfileName,
  completionState,
  consumedTotals,
  mealRows,
  plannedTotals,
  selectedDay,
  targetTotals,
}: {
  activeProfileKey: string;
  activeProfileMeta: string;
  activeProfileName: string;
  completionState: MealCompletionState;
  consumedTotals: InsightsTotals;
  mealRows: MealContribution[];
  plannedTotals?: InsightsTotals;
  selectedDay: InsightsDaySelection;
  targetTotals?: InsightsTotals;
}): DailyProgressSaveInput {
  const meals = mealRows.map((meal) => {
    const mealKey = getMealContributionKey(meal);
    return {
      carbs_g: numberValue(meal.carbs_g),
      display_name: meal.display_name || formatMealSlot(normalizeSlot(meal.slot)),
      eaten: Boolean(completionState[mealKey]),
      fat_g: numberValue(meal.fat_g),
      kcal: numberValue(meal.kcal),
      meal_key: mealKey,
      protein_g: numberValue(meal.protein_g),
      recipe_id: meal.recipe_id || "",
      slot: normalizeSlot(meal.slot),
    };
  });
  return {
    consumed: normalizedTotals(consumedTotals),
    planned: normalizedTotals(plannedTotals),
    target: normalizedTotals(targetTotals ?? plannedTotals),
    meal_completion: {
      completed_meal_keys: meals
        .filter((meal) => meal.eaten)
        .map((meal) => String(meal.meal_key)),
      meals,
    },
    day_snapshot: {
      active_profile_key: activeProfileKey,
      active_profile_meta: activeProfileMeta,
      active_profile_name: activeProfileName,
      meals,
      selected_day: selectedDay,
    },
  };
}

function normalizedTotals(totals?: InsightsTotals): Required<InsightsTotals> {
  return {
    carbs_g: numberValue(totals?.carbs_g),
    fat_g: numberValue(totals?.fat_g),
    kcal: numberValue(totals?.kcal),
    protein_g: numberValue(totals?.protein_g),
  };
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
    fontSize: 12,
    fontWeight: "800",
    lineHeight: 15,
    maxWidth: "48%",
    overflow: "hidden",
    paddingHorizontal: 8,
    paddingVertical: 3,
    textAlign: "center",
  },
  kcalValue: {
    color: "#1B2430",
    fontSize: 18,
    fontWeight: "900",
    lineHeight: 23,
  },
  consistencyLabel: {
    color: colors.muted,
    fontSize: 10,
    fontWeight: "800",
    lineHeight: 13,
    textAlign: "center",
  },
  consistencyMetricBlock: {
    alignItems: "center",
    flex: 1,
    gap: 2,
  },
  consistencyMetrics: {
    alignItems: "flex-start",
    flexDirection: "row",
    gap: 8,
  },
  consistencyProgressFill: {
    backgroundColor: "#8FBE63",
    borderRadius: 8,
    height: "100%",
  },
  consistencyProgressTrack: {
    backgroundColor: "#EEF2E9",
    borderRadius: 8,
    height: 7,
    overflow: "hidden",
  },
  consistencyValue: {
    color: "#1B2430",
    fontSize: 18,
    fontWeight: "900",
    lineHeight: 22,
    textAlign: "center",
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
  metricChip: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: "#DDEAD3",
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 34,
    minWidth: 0,
    paddingHorizontal: 6,
  },
  metricChipActive: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  metricChipText: {
    color: colors.accent,
    fontSize: 11,
    fontWeight: "900",
    textAlign: "center",
  },
  metricChipTextActive: {
    color: "#FFFFFF",
  },
  metricSelector: {
    flexDirection: "row",
    gap: 6,
  },
  patternArrowButton: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    height: 28,
    justifyContent: "center",
    width: 28,
  },
  patternArrowButtonDisabled: {
    borderColor: "#DDEAD3",
  },
  patternArrowText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "900",
    lineHeight: 16,
  },
  patternArrowTextDisabled: {
    color: "#A7B79D",
  },
  patternCell: {
    alignItems: "center",
    borderRadius: 7,
    borderWidth: 1,
    flex: 1,
    height: 24,
    justifyContent: "center",
    minWidth: 0,
  },
  patternCells: {
    flex: 1,
    flexDirection: "row",
    gap: 5,
    minWidth: 0,
  },
  patternDayCell: {
    backgroundColor: "#F7FBF2",
    borderColor: "#DDEAD3",
  },
  patternDayLabel: {
    color: colors.muted,
  },
  patternDayText: {
    color: colors.muted,
    fontSize: 10,
    fontWeight: "900",
    lineHeight: 12,
  },
  patternHeader: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  patternMetricLabel: {
    color: "#1B2430",
    flexShrink: 0,
    fontSize: 11,
    fontWeight: "900",
    lineHeight: 15,
    width: 62,
  },
  patternNav: {
    alignItems: "center",
    flexDirection: "row",
    gap: 6,
  },
  patternRangeText: {
    color: colors.muted,
    fontSize: 11,
    fontWeight: "900",
    lineHeight: 15,
    minWidth: 58,
    textAlign: "center",
  },
  patternRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 9,
  },
  patternRows: {
    gap: 7,
  },
  pressed: {
    opacity: 0.82,
  },
  progressActionRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  progressButtonDisabled: {
    backgroundColor: "#F3F4F6",
    borderColor: "#D1D5DB",
  },
  progressButtonTextDisabled: {
    color: "#9CA3AF",
  },
  progressControlCard: {
    backgroundColor: "#F7FBF2",
    borderColor: "#DDEAD3",
    borderRadius: 20,
    borderWidth: 1,
    gap: 12,
    paddingHorizontal: 14,
    paddingVertical: 13,
  },
  progressErrorText: {
    color: "#B45309",
    fontSize: 12,
    fontWeight: "800",
    lineHeight: 16,
  },
  progressPrimaryButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 12,
  },
  progressPrimaryText: {
    color: "#FFFFFF",
    fontSize: 13,
    fontWeight: "900",
  },
  progressSecondaryButton: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 12,
  },
  progressSecondaryText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "900",
  },
  progressStatusLabel: {
    color: "#1B2430",
    fontSize: 13,
    fontWeight: "900",
    lineHeight: 17,
  },
  progressStatusRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  progressStatusText: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "800",
    lineHeight: 16,
  },
  progressStatusTextBlock: {
    flex: 1,
    gap: 2,
    minWidth: 0,
  },
  progressSavedButtonDisabled: {
    backgroundColor: "#F3F4F6",
    borderColor: "#D1D5DB",
  },
  progressSavedButtonText: {
    color: "#9CA3AF",
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
  rangeButton: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    height: 28,
    justifyContent: "center",
    width: 28,
  },
  rangeButtonDisabled: {
    borderColor: "#DDEAD3",
  },
  rangeButtonText: {
    color: colors.accent,
    fontSize: 17,
    fontWeight: "900",
    lineHeight: 20,
  },
  rangeButtonTextDisabled: {
    color: "#A7B79D",
  },
  rangeLabel: {
    color: "#1B2430",
    flexShrink: 1,
    fontSize: 11,
    fontWeight: "900",
    lineHeight: 14,
    minWidth: 76,
    textAlign: "center",
  },
  rangeSelector: {
    alignItems: "center",
    backgroundColor: "#F7FBF2",
    borderColor: "#DDEAD3",
    borderRadius: 10,
    borderWidth: 1,
    flexDirection: "row",
    gap: 4,
    paddingHorizontal: 4,
    paddingVertical: 4,
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
  trendBarColumn: {
    alignItems: "center",
    flex: 1,
    minWidth: 0,
  },
  trendBarFill: {
    borderRadius: 8,
    minHeight: 5,
    width: "100%",
  },
  trendBarTrack: {
    backgroundColor: "#F3F6EF",
    borderRadius: 8,
    height: 86,
    justifyContent: "flex-end",
    overflow: "hidden",
    position: "relative",
    width: "100%",
  },
  trendCard: {
    borderRadius: 20,
    elevation: 1,
    gap: 12,
    paddingHorizontal: 14,
    paddingVertical: 14,
    shadowOpacity: 0.05,
  },
  trendChart: {
    alignItems: "flex-end",
    flexDirection: "row",
    gap: 4,
    minHeight: 86,
  },
  trendChartFooter: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
  },
  trendHelperText: {
    color: colors.mutedSoft,
    fontSize: 11,
    fontWeight: "700",
    lineHeight: 15,
  },
  trendLegend: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
    width: "100%",
  },
  trendLegendDot: {
    borderRadius: 999,
    height: 8,
    width: 8,
  },
  trendLegendItem: {
    alignItems: "center",
    flex: 1,
    flexDirection: "row",
    gap: 5,
    justifyContent: "flex-start",
  },
  trendLegendItemCenter: {
    justifyContent: "center",
  },
  trendLegendItemRight: {
    justifyContent: "flex-end",
  },
  trendTargetMarker: {
    backgroundColor: "#D6E6CA",
    borderRadius: 2,
    height: 2,
    left: -1,
    position: "absolute",
    right: -1,
  },
  trendTinyText: {
    color: colors.mutedSoft,
    fontSize: 10,
    fontWeight: "800",
    lineHeight: 13,
  },
  trendsEmptyCard: {
    gap: 12,
  },
  trendsEmptyState: {
    backgroundColor: "#F7FBF2",
    borderColor: "#DDEAD3",
    borderRadius: 16,
    borderWidth: 1,
    gap: 3,
    paddingHorizontal: 13,
    paddingVertical: 12,
  },
  trendsEmptyText: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
    lineHeight: 18,
  },
  trendsEmptyTitle: {
    color: "#1B2430",
    fontSize: 14,
    fontWeight: "900",
    lineHeight: 18,
  },
  trendCardHeader: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
    justifyContent: "space-between",
  },
  trendCardTitle: {
    color: "#1B2430",
    flex: 1,
    fontSize: 17,
    fontWeight: "900",
    lineHeight: 22,
    minWidth: 0,
  },
  trendsSection: {
    gap: 12,
  },
});
