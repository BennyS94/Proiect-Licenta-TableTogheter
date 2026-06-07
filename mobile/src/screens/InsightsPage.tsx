import type { ReactNode } from "react";
import { StyleSheet, Text, View } from "react-native";

import { AppCard } from "../components/ui/AppCard";
import { AppScreen } from "../components/ui/AppScreen";
import { DaySelector } from "../components/ui/DaySelector";
import { EmptyState } from "../components/ui/EmptyState";
import { MacroBar } from "../components/ui/MacroBar";
import { MacroDonutPlaceholder } from "../components/ui/MacroDonutPlaceholder";
import { SectionHeader } from "../components/ui/SectionHeader";
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

export function InsightsPage({
  activeProfileMeta,
  activeProfileName,
  dayIndexes,
  hasMembers,
  hasPlan,
  householdName,
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
  const kcalPercent = percentOfTarget(totals?.kcal, targetTotals?.kcal);

  return (
    <AppScreen>
      <View style={styles.header}>
        <Text style={styles.title}>Insights</Text>
        <Text style={styles.subtitle}>{householdName}</Text>
        <Text style={styles.meta}>{activeProfileName}</Text>
        <Text style={styles.meta}>{activeProfileMeta}</Text>
      </View>

      {profileSelector}

      <DaySelector
        dayIndexes={dayIndexes}
        includeAverage
        onSelect={onSelectDay}
        selected={selectedDay}
      />

      <AppCard>
        <SectionHeader title="Daily Balance" meta={selectedDay === "average" ? "Average" : `Day ${selectedDay}`} />
        <View style={styles.balanceRow}>
          <View style={styles.balanceText}>
            <Text style={styles.kcal}>
              {formatNumber(totals?.kcal)} kcal
              {targetTotals?.kcal ? ` / ${formatNumber(targetTotals.kcal)}` : ""}
            </Text>
            <Text style={styles.meta}>
              {kcalPercent !== null ? `${Math.round(kcalPercent)}% of target` : "Target unavailable"}
            </Text>
          </View>
        </View>
        <MacroDonutPlaceholder
          carbsPercent={macroPercents.carbs}
          fatPercent={macroPercents.fat}
          proteinPercent={macroPercents.protein}
        />
      </AppCard>

      <AppCard>
        <SectionHeader title="Macro Targets" />
        <View style={styles.barList}>
          <MacroBar
            actual={totals?.kcal}
            color={colors.accent}
            label="Calories"
            target={targetTotals?.kcal}
          />
          <MacroBar
            actual={totals?.protein_g}
            color="#D64B4B"
            label="Protein"
            target={targetTotals?.protein_g}
            unit="g"
          />
          <MacroBar
            actual={totals?.carbs_g}
            color="#E58A2F"
            label="Carbs"
            target={targetTotals?.carbs_g}
            unit="g"
          />
          <MacroBar
            actual={totals?.fat_g}
            color="#E2C84C"
            label="Fats"
            target={targetTotals?.fat_g}
            unit="g"
          />
        </View>
      </AppCard>

      <AppCard>
        <SectionHeader title="Meal Contribution" />
        <View style={styles.mealList}>
          {mealContributions.map((meal) => (
            <View key={meal.slot} style={styles.mealRow}>
              <Text style={styles.mealSlot}>{titleize(meal.slot)}</Text>
              <Text style={styles.mealValue}>
                {formatNumber(meal.kcal)} kcal - {formatNumber(meal.protein_g)}g protein
              </Text>
            </View>
          ))}
        </View>
      </AppCard>

      <AppCard>
        <SectionHeader title="Micronutrient Snapshot" meta="Concept / Estimated" />
        <Text style={styles.bodyText}>
          Micronutrient coverage is shown as a future-oriented preview. Values depend on
          available recipe data.
        </Text>
      </AppCard>

      <AppCard>
        <SectionHeader title="Plan Insight" />
        <Text style={styles.bodyText}>{buildPlanInsight(totals, targetTotals)}</Text>
      </AppCard>
    </AppScreen>
  );
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

function buildPlanInsight(totals?: InsightsTotals, targets?: InsightsTotals): string {
  const kcalRatio = ratio(totals?.kcal, targets?.kcal);
  const proteinRatio = ratio(totals?.protein_g, targets?.protein_g);
  if (kcalRatio !== null && proteinRatio !== null) {
    if (kcalRatio >= 0.9 && kcalRatio <= 1.1 && proteinRatio >= 0.85) {
      return "This plan is close to the selected calorie target. Protein coverage is strong, while carbs and fats remain within a reasonable range.";
    }
    if (proteinRatio < 0.85) {
      return "This plan has usable meal structure, but protein coverage may need attention for the selected profile.";
    }
  }
  return "This plan is ready for a quick visual review. Target comparisons depend on the nutrition fields returned by the backend.";
}

function percentOfTarget(actual?: number, target?: number): number | null {
  const parsedActual = numberValue(actual);
  const parsedTarget = numberValue(target);
  if (parsedTarget <= 0) {
    return null;
  }
  return (parsedActual / parsedTarget) * 100;
}

function ratio(actual?: number, target?: number): number | null {
  const parsedTarget = numberValue(target);
  if (parsedTarget <= 0) {
    return null;
  }
  return numberValue(actual) / parsedTarget;
}

function formatNumber(value: unknown): string {
  const parsed = numberValue(value);
  if (parsed === 0 && value == null) {
    return "-";
  }
  return parsed ? String(Math.round(parsed)) : parsed === 0 ? "0" : "-";
}

function numberValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const styles = StyleSheet.create({
  balanceRow: {
    gap: 8,
  },
  balanceText: {
    gap: 2,
  },
  barList: {
    gap: 12,
  },
  bodyText: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 21,
  },
  header: {
    gap: 4,
    paddingTop: 4,
  },
  kcal: {
    color: colors.text,
    fontSize: 24,
    fontWeight: "900",
  },
  mealList: {
    gap: 10,
  },
  mealRow: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  mealSlot: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
  },
  mealValue: {
    color: colors.muted,
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "700",
    textAlign: "right",
  },
  meta: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
  },
  subtitle: {
    color: colors.accent,
    fontSize: 17,
    fontWeight: "800",
  },
  title: {
    color: colors.text,
    fontSize: 30,
    fontWeight: "900",
  },
});
