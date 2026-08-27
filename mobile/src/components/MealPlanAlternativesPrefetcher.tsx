import { useEffect } from "react";

import type {
  DemoMemberProfile,
  GeneratedDay,
  GeneratedMeal,
  HouseholdMeal,
  HouseholdMemberMenu,
  HouseholdPlanGenerateResponse,
  IndividualPlanGenerateResponse,
  MealReplacementScope,
} from "../types/api";
import {
  prefetchRecipeAlternativePreviews,
  type RecipeAlternativePreviewPrefetchInput,
} from "./RecipeAlternativesPanel";

type MealPlanAlternativesPrefetcherProps = {
  datasetProfile?: string;
  generationMode: "individual" | "household";
  householdId?: string;
  householdMembers?: DemoMemberProfile[];
  householdPlan?: HouseholdPlanGenerateResponse | null;
  individualMemberProfile?: DemoMemberProfile;
  individualMemberProfileId?: string;
  individualPlan?: IndividualPlanGenerateResponse | null;
  planId?: string;
  preferredDayIndex?: number;
  preferredHouseholdMemberId?: string;
};

const MEAL_SLOT_ORDER = ["breakfast", "lunch", "snack", "dinner"];

export function MealPlanAlternativesPrefetcher({
  datasetProfile = "current",
  generationMode,
  householdId,
  householdMembers = [],
  householdPlan,
  individualMemberProfile,
  individualMemberProfileId,
  individualPlan,
  planId,
  preferredDayIndex = 1,
  preferredHouseholdMemberId,
}: MealPlanAlternativesPrefetcherProps) {
  useEffect(() => {
    if (!planId) {
      return;
    }

    const queue =
      generationMode === "household"
        ? buildHouseholdPrefetchQueue({
            datasetProfile,
            householdId,
            householdMembers,
            plan: householdPlan,
            planId,
            preferredDayIndex,
            preferredHouseholdMemberId,
          })
        : buildIndividualPrefetchQueue({
            datasetProfile,
            householdId,
            memberProfile: individualMemberProfile,
            memberProfileId: individualMemberProfileId,
            plan: individualPlan,
            planId,
            preferredDayIndex,
          });

    if (!queue.length) {
      return;
    }

    let isCurrent = true;

    async function runQueue() {
      for (const item of queue) {
        if (!isCurrent) {
          return;
        }
        await prefetchRecipeAlternativePreviews(item);
      }
    }

    void runQueue();

    return () => {
      isCurrent = false;
    };
  }, [
    datasetProfile,
    generationMode,
    householdId,
    householdMembers,
    householdPlan,
    individualMemberProfile,
    individualMemberProfileId,
    individualPlan,
    planId,
    preferredDayIndex,
    preferredHouseholdMemberId,
  ]);

  return null;
}

function buildIndividualPrefetchQueue({
  datasetProfile,
  householdId,
  memberProfile,
  memberProfileId,
  plan,
  planId,
  preferredDayIndex,
}: {
  datasetProfile: string;
  householdId?: string;
  memberProfile?: DemoMemberProfile;
  memberProfileId?: string;
  plan?: IndividualPlanGenerateResponse | null;
  planId: string;
  preferredDayIndex: number;
}): RecipeAlternativePreviewPrefetchInput[] {
  const days = [...(plan?.daily_plan ?? [])].sort((left, right) =>
    comparePreferredDay(left.day_index, right.day_index, preferredDayIndex),
  );

  return uniquePrefetchItems(
    days.flatMap((day) => {
      const dayIndex = normalizeDayIndex(day.day_index);
      return getIndividualMeals(day).map((meal) => ({
        datasetProfile,
        dayIndex,
        generationType: "individual" as const,
        householdId,
        memberProfile,
        memberProfileId,
        planId,
        replaceScope: "individual_meal" as const,
        slot: getMealSlot(meal),
        sourceRecipeId: getMealRecipeId(meal),
      }));
    }),
  );
}

function buildHouseholdPrefetchQueue({
  datasetProfile,
  householdId,
  householdMembers,
  plan,
  planId,
  preferredDayIndex,
  preferredHouseholdMemberId,
}: {
  datasetProfile: string;
  householdId?: string;
  householdMembers: DemoMemberProfile[];
  plan?: HouseholdPlanGenerateResponse | null;
  planId: string;
  preferredDayIndex: number;
  preferredHouseholdMemberId?: string;
}): RecipeAlternativePreviewPrefetchInput[] {
  const memberOrder = new Map(
    householdMembers.map((member, index) => [getMemberId(member), index]),
  );
  const menus = [...(plan?.per_member_menus ?? [])].sort((left, right) => {
    const leftMemberId = getMemberId(left);
    const rightMemberId = getMemberId(right);
    const leftPreferred = leftMemberId === preferredHouseholdMemberId ? 0 : 1;
    const rightPreferred = rightMemberId === preferredHouseholdMemberId ? 0 : 1;
    if (leftPreferred !== rightPreferred) {
      return leftPreferred - rightPreferred;
    }

    const dayCompare = comparePreferredDay(
      left.day_index ?? left.day,
      right.day_index ?? right.day,
      preferredDayIndex,
    );
    if (dayCompare !== 0) {
      return dayCompare;
    }

    return (
      (memberOrder.get(leftMemberId) ?? Number.MAX_SAFE_INTEGER) -
      (memberOrder.get(rightMemberId) ?? Number.MAX_SAFE_INTEGER)
    );
  });

  return uniquePrefetchItems(
    menus.flatMap((menu) => {
      const memberId = getMemberId(menu);
      const dayIndex = normalizeDayIndex(menu.day_index ?? menu.day);
      return getHouseholdMeals(menu).map((meal) => ({
        datasetProfile,
        dayIndex,
        generationType: "household" as const,
        householdId,
        memberId,
        memberProfileId: memberId,
        planId,
        replaceScope: replacementScopeFromMeal(meal),
        slot: getMealSlot(meal),
        sourceRecipeId: getMealRecipeId(meal),
      }));
    }),
  );
}

function uniquePrefetchItems(
  items: RecipeAlternativePreviewPrefetchInput[],
): RecipeAlternativePreviewPrefetchInput[] {
  const seen = new Set<string>();
  return items.filter((item) => {
    if (!item.sourceRecipeId || !item.slot) {
      return false;
    }
    const key = [
      item.generationType,
      item.planId,
      item.dayIndex,
      item.memberId,
      item.memberProfileId,
      item.slot,
      item.sourceRecipeId,
      item.replaceScope,
    ].join("::");
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function getIndividualMeals(day: GeneratedDay): GeneratedMeal[] {
  const meals = Array.isArray(day.selected_meals)
    ? day.selected_meals
    : Array.isArray(day.meals)
      ? day.meals
      : [];
  return sortMealsBySlot(meals.filter(isRecord) as GeneratedMeal[]);
}

function getHouseholdMeals(menu: HouseholdMemberMenu): HouseholdMeal[] {
  const meals = Array.isArray(menu.meals) ? menu.meals : menu.selected_meals;
  return sortMealsBySlot((meals ?? []).filter(isRecord) as HouseholdMeal[]);
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

function comparePreferredDay(left: unknown, right: unknown, preferredDayIndex: number): number {
  const leftDay = normalizeDayIndex(left);
  const rightDay = normalizeDayIndex(right);
  const leftPreferred = leftDay === preferredDayIndex ? 0 : 1;
  const rightPreferred = rightDay === preferredDayIndex ? 0 : 1;
  if (leftPreferred !== rightPreferred) {
    return leftPreferred - rightPreferred;
  }
  return leftDay - rightDay;
}

function replacementScopeFromMeal(meal: HouseholdMeal): MealReplacementScope {
  return stringValue(meal.meal_scope) === "shared"
    ? "household_shared_meal"
    : "household_member_meal";
}

function getMealSlot(meal: { slot?: unknown }): string {
  return String(meal.slot ?? "").trim();
}

function getMealRecipeId(meal: { recipe_id?: unknown }): string {
  return String(meal.recipe_id ?? "").trim();
}

function getMemberId(value: { member_id?: unknown; member_profile_id?: unknown }): string {
  return String(value.member_profile_id ?? value.member_id ?? "").trim();
}

function normalizeDayIndex(value: unknown): number {
  const numeric = typeof value === "number" && Number.isFinite(value) ? value : 1;
  return numeric <= 0 ? numeric + 1 : numeric;
}

function getMealSlotOrderIndex(slot: unknown): number {
  const normalized = String(slot ?? "").trim().toLowerCase();
  const index = MEAL_SLOT_ORDER.indexOf(normalized);
  return index >= 0 ? index : MEAL_SLOT_ORDER.length;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}
