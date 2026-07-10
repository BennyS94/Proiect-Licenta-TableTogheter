import type {
  AuthAccount,
  DailyProgressSnapshot,
  MealReplacementResponse,
  GroceryListItem,
  HouseholdMeal,
  HouseholdMemberMacroSummary,
  HouseholdMemberMenu,
  HouseholdMemberTarget,
  HouseholdPlanGenerateResponse,
  MemberProfileResponse,
  RecipeAlternativesResponse,
} from "../../types/api";

// Fixture temporar pentru simularea TableTogether. Nu este logica de productie.
const DIMA_EMAIL = "dima.household@tabletogether.app";
const DIMA_HOUSEHOLD_NAME = "Dima Household";
const PLAN_ID = "dima_household_presentation_plan";
const PRESENTATION_PROGRESS_HISTORY_DAYS = 30;
const PRESENTATION_PROGRESS_START_DATE_UTC = Date.UTC(2026, 5, 1, 20, 0, 0);

const MEMBER_IDS = {
  adrian: "member_profile_dima_adrian",
  alice: "member_profile_dima_alice",
  marius: "member_profile_dima_marius",
} as const;

const TARGETS = {
  [MEMBER_IDS.alice]: { carbs_g: 240, fat_g: 60, kcal: 1900, protein_g: 105 },
  [MEMBER_IDS.adrian]: { carbs_g: 190, fat_g: 55, kcal: 1950, protein_g: 155 },
  [MEMBER_IDS.marius]: { carbs_g: 360, fat_g: 80, kcal: 2900, protein_g: 170 },
} as const;

type MacroTotals = {
  carbs_g: number;
  fat_g: number;
  kcal: number;
  protein_g: number;
};

type MealFixture = MacroTotals & {
  display_name: string;
  ingredients: string[];
  meal_scope: "individual" | "shared";
  portion_multiplier: number;
  recipe_id: string;
  slot: "breakfast" | "lunch" | "snack" | "dinner";
  time_min: number;
};

type MemberDayFixture = {
  day_index: number;
  meals: MealFixture[];
  totals: MacroTotals;
};

export function isDimaPresentationFixtureAccount(account: AuthAccount | null): boolean {
  if (!account) {
    return false;
  }
  return (
    normalize(account.email) === DIMA_EMAIL ||
    normalize(account.household_display_name) === normalize(DIMA_HOUSEHOLD_NAME)
  );
}

export function buildDimaPresentationFixturePlan(
  profiles: MemberProfileResponse[],
): HouseholdPlanGenerateResponse | null {
  const orderedProfiles = orderDimaProfiles(profiles);
  if (orderedProfiles.length !== 3) {
    return null;
  }
  const householdId = orderedProfiles[0]?.household_id || "household_dima";
  const perMemberMenus = orderedProfiles.flatMap((profile) =>
    getMemberDays(profile.member_profile_id).map((day) =>
      memberDayToMenu(profile.member_profile_id, day),
    ),
  );
  const memberTargets = orderedProfiles.map((profile) =>
    buildMemberTarget(profile.member_profile_id, profile.display_name),
  );
  const memberMacroSummaries = perMemberMenus.map(menuToMacroSummary);

  return {
    dataset_profile: "v1_2_demo_final",
    days: 3,
    generation_type: "household",
    household_grocery_list: buildGroceryList(),
    household_id: householdId,
    household_plan_id: PLAN_ID,
    member_macro_summaries: memberMacroSummaries,
    member_targets: memberTargets,
    per_member_menus: perMemberMenus,
    plan_id: PLAN_ID,
    selected_members: orderedProfiles.map((profile) => ({
      ...profile,
      display_name: cleanDimaName(profile.display_name),
      member_id: profile.member_profile_id,
      member_profile_id: profile.member_profile_id,
      profile_name: cleanDimaName(profile.display_name),
    })),
    status: "ok",
  };
}

export function getDimaPresentationProfileIds(
  profiles: MemberProfileResponse[],
): string[] {
  return orderDimaProfiles(profiles).map((profile) => profile.member_profile_id);
}

export function buildDimaPresentationProgressSnapshots(
  profiles: MemberProfileResponse[],
  householdId: string,
): DailyProgressSnapshot[] {
  return orderDimaProfiles(profiles).flatMap((profile) =>
    buildDimaPresentationProgressDays(profile.member_profile_id).map(({ day, historyDayIndex }) => {
      const savedAt = buildPresentationProgressSavedAt(historyDayIndex);
      const completedMeals = getCompletedMealsForPresentationProgress(
        profile.member_profile_id,
        day,
        historyDayIndex,
      );
      const consumed = scaleTotals(
        sumMealTotals(completedMeals),
        getPresentationProgressMultiplier(profile.member_profile_id, historyDayIndex),
      );
      return {
        consumed,
        created_at: savedAt,
        day_index: historyDayIndex,
        day_snapshot: {
          active_profile_name: cleanDimaName(profile.display_name),
          selected_day: historyDayIndex,
        },
        household_id: householdId,
        meal_completion: {
          completed_meal_keys: completedMeals.map(
            (meal) => `${meal.slot}:${meal.recipe_id}`,
          ),
        },
        member_profile_id: profile.member_profile_id,
        plan_id: PLAN_ID,
        planned: day.totals,
        progress_id: `dima_progress_${profile.member_profile_id}_day_${historyDayIndex}`,
        saved_at: savedAt,
        target: TARGETS[profile.member_profile_id as keyof typeof TARGETS],
        updated_at: savedAt,
      };
    }),
  );
}

function getCompletedMealsForPresentationProgress(
  memberProfileId: string,
  day: MemberDayFixture,
  historyDayIndex = day.day_index,
): MealFixture[] {
  const skippedSlotsByHistoryDay: Record<string, Record<number, Array<MealFixture["slot"]>>> = {
    [MEMBER_IDS.alice]: {
      8: ["snack"],
      17: ["snack"],
      26: ["snack"],
    },
    [MEMBER_IDS.adrian]: {
      10: ["snack"],
      21: ["snack"],
    },
    [MEMBER_IDS.marius]: {
      6: ["snack"],
      18: ["snack"],
      28: ["snack"],
    },
  };
  const skippedSlots = skippedSlotsByHistoryDay[memberProfileId]?.[historyDayIndex] ?? [];
  return day.meals.filter((meal) => !skippedSlots.includes(meal.slot));
}

function buildDimaPresentationProgressDays(
  memberProfileId: string,
): Array<{ day: MemberDayFixture; historyDayIndex: number }> {
  const days = getMemberDays(memberProfileId);
  return Array.from({ length: PRESENTATION_PROGRESS_HISTORY_DAYS }, (_, index) => ({
    day: days[index % days.length],
    historyDayIndex: index + 1,
  }));
}

function buildPresentationProgressSavedAt(historyDayIndex: number): string {
  const date = new Date(PRESENTATION_PROGRESS_START_DATE_UTC);
  date.setUTCDate(date.getUTCDate() + historyDayIndex - 1);
  return date.toISOString().replace(".000Z", "+00:00");
}

function getPresentationProgressMultiplier(
  memberProfileId: string,
  historyDayIndex: number,
): number {
  const memberOffset =
    memberProfileId === MEMBER_IDS.alice ? 0 : memberProfileId === MEMBER_IDS.adrian ? 1 : 2;
  const pattern = [0.96, 1.01, 0.99, 1.04, 0.98, 1.02, 1];
  return pattern[(historyDayIndex + memberOffset) % pattern.length];
}

function sumMealTotals(meals: MealFixture[]): MacroTotals {
  return meals.reduce(
    (totals, meal) => ({
      carbs_g: totals.carbs_g + meal.carbs_g,
      fat_g: totals.fat_g + meal.fat_g,
      kcal: totals.kcal + meal.kcal,
      protein_g: totals.protein_g + meal.protein_g,
    }),
    { carbs_g: 0, fat_g: 0, kcal: 0, protein_g: 0 },
  );
}

function scaleTotals(totals: MacroTotals, multiplier: number): MacroTotals {
  return {
    carbs_g: roundMacro(totals.carbs_g * multiplier),
    fat_g: roundMacro(totals.fat_g * multiplier),
    kcal: Math.round(totals.kcal * multiplier),
    protein_g: roundMacro(totals.protein_g * multiplier),
  };
}

function orderDimaProfiles(profiles: MemberProfileResponse[]): MemberProfileResponse[] {
  const byId = new Map(profiles.map((profile) => [profile.member_profile_id, profile]));
  return [MEMBER_IDS.alice, MEMBER_IDS.adrian, MEMBER_IDS.marius]
    .map((id) => byId.get(id))
    .filter((profile): profile is MemberProfileResponse => Boolean(profile));
}

function getMemberDays(memberProfileId: string): MemberDayFixture[] {
  if (memberProfileId === MEMBER_IDS.alice) {
    return ALICE_DAYS;
  }
  if (memberProfileId === MEMBER_IDS.adrian) {
    return ADRIAN_DAYS;
  }
  return MARIUS_DAYS;
}

function memberDayToMenu(
  memberProfileId: string,
  day: MemberDayFixture,
): HouseholdMemberMenu {
  return {
    daily_totals: day.totals,
    day: day.day_index,
    day_index: day.day_index,
    meals: day.meals.map((meal) => {
      const presentationAlternatives = buildPresentationAlternatives(memberProfileId, meal);
      const cookingSteps = cookingStepsFor(meal.slot, meal.display_name);

      return {
        ...meal,
        cooking_steps: cookingSteps,
        directions_step_count: cookingSteps.length,
        effective_time_min: meal.time_min,
        meal_scope: meal.meal_scope,
        member_profile_id: memberProfileId,
        presentation_alternatives_response: presentationAlternatives.response,
        presentation_preview_responses: presentationAlternatives.previews,
        time_min: meal.time_min,
        total_time_min: meal.time_min,
      };
    }) as HouseholdMeal[],
    member_id: memberProfileId,
    member_profile_id: memberProfileId,
    selected_meals: day.meals as HouseholdMeal[],
    totals: day.totals,
  };
}

function buildMemberTarget(
  memberProfileId: string,
  displayName: string,
): HouseholdMemberTarget {
  const target = TARGETS[memberProfileId as keyof typeof TARGETS];
  return {
    carbs_g: target.carbs_g,
    display_name: cleanDimaName(displayName),
    fat_g: target.fat_g,
    kcal: target.kcal,
    member_id: memberProfileId,
    member_profile_id: memberProfileId,
    protein_g: target.protein_g,
    target_carbs_g: target.carbs_g,
    target_fat_g: target.fat_g,
    target_kcal: target.kcal,
    target_protein_g: target.protein_g,
  };
}

function menuToMacroSummary(menu: HouseholdMemberMenu): HouseholdMemberMacroSummary {
  const memberId = String(menu.member_profile_id ?? menu.member_id ?? "");
  const target = TARGETS[memberId as keyof typeof TARGETS];
  const totals = menu.totals ?? {};
  return {
    carbs_g: totals.carbs_g,
    day: menu.day_index,
    day_index: menu.day_index,
    fat_g: totals.fat_g,
    kcal: totals.kcal,
    member_id: memberId,
    member_profile_id: memberId,
    protein_g: totals.protein_g,
    target_carbs_g: target.carbs_g,
    target_fat_g: target.fat_g,
    target_kcal: target.kcal,
    target_protein_g: target.protein_g,
    targets: target,
    totals,
  };
}

function cookingStepsFor(slot: MealFixture["slot"], displayName: string): string[] {
  if (displayName.toLowerCase().includes("yogurt")) {
    return [
      "Add Greek yogurt to a bowl.",
      "Mix in oats and fruit.",
      "Add nuts or peanut butter if included.",
      "Serve cold.",
    ];
  }
  if (displayName.toLowerCase().includes("omelette")) {
    return [
      "Whisk the eggs or egg whites.",
      "Cook the vegetables in a non-stick pan.",
      "Add the eggs and cook until set.",
      "Serve with toast and yogurt if included.",
    ];
  }
  if (displayName.toLowerCase().includes("toast")) {
    return [
      "Toast the bread.",
      "Add cottage cheese and the prepared toppings.",
      "Cook the egg if included.",
      "Serve with fruit if included.",
    ];
  }
  if (slot === "lunch") {
    return [
      "Prepare the grain or pasta base.",
      "Add vegetables and sauce.",
      "Add tofu, legumes or chicken depending on the member.",
      "Season and serve warm.",
    ];
  }
  if (slot === "snack") {
    return [
      "Wash and slice the fruit.",
      "Add yogurt or cottage cheese.",
      "Top with nuts or peanut butter if included.",
      "Serve as a simple snack.",
    ];
  }
  return [
    "Cook the carbohydrate base.",
    "Prepare the vegetables.",
    "Cook tofu for Alice or chicken for Adrian and Marius.",
    "Combine with sauce and adjust the portion by member.",
    "Serve warm.",
  ];
}

function buildPresentationAlternatives(
  memberProfileId: string,
  meal: MealFixture,
): {
  response: RecipeAlternativesResponse;
  previews: Record<string, MealReplacementResponse>;
} {
  const templates = presentationAlternativeTemplates(memberProfileId, meal.slot);
  const alternatives = templates.map((template, index) => {
    const alternativeRecipeId = `${meal.recipe_id}_alt_${index + 1}`;
    return {
      approval_status: "approved",
      display_name: template.name,
      recipe_id: alternativeRecipeId,
      similarity_score: template.similarity,
      time_delta_min: template.timeDeltaMin,
      why_similar: ["same_slot", "presentation_fixture"],
    };
  });
  const previews = Object.fromEntries(
    templates.map((template, index) => {
      const alternativeRecipeId = `${meal.recipe_id}_alt_${index + 1}`;
      const alternativeMeal = presentationAlternativeMeal(meal, template, alternativeRecipeId);
      const currentMeal = mealToGeneratedMeal(meal);
      const delta = {
        carbs_g: roundMacro((alternativeMeal.carbs_g ?? 0) - meal.carbs_g),
        fat_g: roundMacro((alternativeMeal.fat_g ?? 0) - meal.fat_g),
        kcal: Math.round((alternativeMeal.kcal ?? 0) - meal.kcal),
        protein_g: roundMacro((alternativeMeal.protein_g ?? 0) - meal.protein_g),
      };

      return [
        alternativeRecipeId,
        {
          approval_status: "approved",
          dry_run: true,
          generation_type: "household",
          impact: {
            grocery_rebuilt: true,
            meal_macro_delta: delta,
          },
          plan_id: PLAN_ID,
          replacement: {
            alternative_meal: alternativeMeal,
            current_meal: currentMeal,
            replace_scope:
              meal.meal_scope === "shared" ? "household_shared_meal" : "household_member_meal",
          },
          replacement_allowed: true,
          source_plan_id: PLAN_ID,
          status: "ok",
        } satisfies MealReplacementResponse,
      ];
    }),
  );

  return {
    previews,
    response: {
      alternatives,
      approval_mode: "approved_only",
      dataset_profile: "v1_2_demo_final",
      recipe_id: meal.recipe_id,
      slot: meal.slot,
      source_recipe: {
        display_name: meal.display_name,
        recipe_id: meal.recipe_id,
      },
      status: "ok",
      summary: {
        returned_count: alternatives.length,
      },
    },
  };
}

function presentationAlternativeTemplates(
  memberProfileId: string,
  slot: MealFixture["slot"],
): Array<{
  name: string;
  kcalFactor: number;
  proteinFactor: number;
  carbFactor: number;
  fatFactor: number;
  similarity: number;
  timeDeltaMin: number;
}> {
  const vegetarian = memberProfileId === MEMBER_IDS.alice;
  const shared = vegetarian
    ? {
        breakfast: ["Cottage Cheese Fruit Bowl", "Vegetable Feta Toast"],
        lunch: ["Lentil Rice Bowl", "Chickpea Pasta Bowl"],
        snack: ["Greek Yogurt Fruit Cup", "Apple Peanut Butter Snack"],
        dinner: ["Tofu Vegetable Stir Fry", "Lentil Tomato Rice Bowl"],
      }
    : {
        breakfast: ["Greek Yogurt Oat Bowl", "Egg and Cottage Cheese Toast"],
        lunch: ["Chicken Rice Bowl", "Turkey Bean Wrap"],
        snack: ["Cottage Cheese Fruit Cup", "Yogurt and Peanut Butter Bowl"],
        dinner: ["Chicken Broccoli Rice Bowl", "Turkey Vegetable Pasta Bowl"],
      };
  const names = shared[slot];

  return names.map((name, index) => ({
    carbFactor: index === 0 ? 0.96 : 1.04,
    fatFactor: index === 0 ? 0.92 : 1.08,
    kcalFactor: index === 0 ? 0.97 : 1.03,
    name,
    proteinFactor: index === 0 ? 1.02 : 0.96,
    similarity: index === 0 ? 0.86 : 0.81,
    timeDeltaMin: index === 0 ? -2 : 4,
  }));
}

function presentationAlternativeMeal(
  meal: MealFixture,
  template: ReturnType<typeof presentationAlternativeTemplates>[number],
  recipeId: string,
): HouseholdMeal {
  const timeMin = Math.max(5, meal.time_min + template.timeDeltaMin);

  return {
    carbs_g: roundMacro(meal.carbs_g * template.carbFactor),
    display_name: template.name,
    effective_time_min: timeMin,
    fat_g: roundMacro(meal.fat_g * template.fatFactor),
    kcal: Math.round(meal.kcal * template.kcalFactor),
    meal_scope: meal.meal_scope,
    portion_multiplier: meal.portion_multiplier,
    protein_g: roundMacro(meal.protein_g * template.proteinFactor),
    recipe_id: recipeId,
    slot: meal.slot,
    time_min: timeMin,
    total_time_min: timeMin,
  };
}

function mealToGeneratedMeal(meal: MealFixture): HouseholdMeal {
  return {
    carbs_g: meal.carbs_g,
    display_name: meal.display_name,
    effective_time_min: meal.time_min,
    fat_g: meal.fat_g,
    kcal: meal.kcal,
    meal_scope: meal.meal_scope,
    portion_multiplier: meal.portion_multiplier,
    protein_g: meal.protein_g,
    recipe_id: meal.recipe_id,
    slot: meal.slot,
    time_min: meal.time_min,
    total_time_min: meal.time_min,
  };
}

function roundMacro(value: number): number {
  return Math.round(value * 10) / 10;
}

function buildGroceryList() {
  const items: GroceryListItem[] = [
    groceryItem("Bananas", "fruits", "~7 pieces", "1 bunch", 8.5),
    groceryItem("Apples", "fruits", "~6 pieces", "1 bag", 9.5),
    groceryItem("Mixed berries", "fruits", "~600 g", "1 pack", 18),
    groceryItem("Avocados", "fruits", "3 pieces", "3 pieces", 16.5),
    groceryItem("Spinach", "vegetables", "~300 g", "1 bag", 9),
    groceryItem("Broccoli", "vegetables", "~700 g", "2 heads", 12),
    groceryItem("Bell peppers", "vegetables", "5 pieces", "5 pieces", 15),
    groceryItem("Zucchini", "vegetables", "3 pieces", "3 pieces", 8.5),
    groceryItem("Tomatoes", "vegetables", "~900 g", "1 kg", 10),
    groceryItem("Lettuce", "vegetables", "1 head", "1 head", 6),
    groceryItem("Mixed vegetables", "vegetables", "~1.2 kg", "2 bags", 20),
    groceryItem("Salad vegetables", "vegetables", "~600 g", "1 pack", 12),
    groceryItem("Onions", "vegetables", "5 pieces", "1 bag", 6.5),
    groceryItem("Garlic", "vegetables", "1 bulb", "1 bulb", 3),
    groceryItem("Lemons", "fruits", "3 pieces", "3 pieces", 6),
    groceryItem("Rolled oats", "carbs_grains", "~600 g", "1 pack", 9),
    groceryItem("Rice", "carbs_grains", "~1.5 kg", "1 bag", 13),
    groceryItem("Wholegrain bread", "carbs_grains", "1 loaf", "1 loaf", 8.5),
    groceryItem("Pasta", "carbs_grains", "~1.2 kg", "3 x 500 g packs", 15),
    groceryItem("Tortillas", "carbs_grains", "1 pack", "1 pack", 11),
    groceryItem("Granola", "carbs_grains", "1 pack", "1 pack", 14),
    groceryItem("Chicken breast", "meat_fish", "~2.2 kg", "~2.5 kg", 58),
    groceryItem("Firm tofu", "legumes_beans", "~900 g", "3 blocks", 27),
    groceryItem("Eggs", "dairy_eggs", "18 pieces", "18 eggs", 22),
    groceryItem("Egg whites", "dairy_eggs", "1 carton", "1 carton", 14),
    groceryItem("Greek yogurt", "dairy_eggs", "~2.2 kg", "2 large tubs", 32),
    groceryItem("Cottage cheese", "dairy_eggs", "~1.2 kg", "3 tubs", 24),
    groceryItem("Cooked chickpeas", "legumes_beans", "3 cans", "3 cans", 15),
    groceryItem("Black beans", "legumes_beans", "3 cans", "3 cans", 15),
    groceryItem("Lentils", "legumes_beans", "3 cans", "3 cans", 14),
    groceryItem("Feta cheese", "dairy_eggs", "~350 g", "2 packs", 17),
    groceryItem("Peanut butter", "oils_fats", "1 jar", "1 jar", 18),
    groceryItem("Walnuts or mixed nuts", "oils_fats", "~300 g", "1 bag", 20),
    groceryItem("Olive oil", "pantry_basics", "500 ml", "1 bottle", 24),
    groceryItem("Soy sauce", "pantry_basics", "250 ml", "1 bottle", 11),
    groceryItem("Tomato sauce", "sauces_canned", "3 jars", "3 jars", 18),
    groceryItem("Spices", "seasonings_spices", "1 small pack", "1 small pack", 8),
    groceryItem("Salt and pepper", "seasonings_spices", "1 set", "1 set", 7),
  ];
  const estimatedTotal = items.reduce((sum, item) => sum + (item.estimated_cost ?? 0), 0);
  return {
    currency: "RON",
    display_items: items,
    estimated_total_cost: estimatedTotal,
    grocery_list_id: "dima_household_grocery_list",
    plan_id: PLAN_ID,
    status: "ok",
    summary: {
      currency: "RON",
      estimated_total_cost: estimatedTotal,
      missing_price_count: 0,
      priced_item_count: items.length,
      total_estimated_cost: estimatedTotal,
    },
    total_estimated_cost: estimatedTotal,
  };
}

function groceryItem(
  displayName: string,
  category: string,
  need: string,
  buy: string,
  cost: number,
): GroceryListItem {
  return {
    category_label: category,
    currency: "RON",
    display_name: displayName,
    display_name_clean: displayName,
    estimated_cost: cost,
    estimated_cost_display: `${cost.toFixed(2)} RON`,
    grocery_item_id: `dima_grocery_${normalize(displayName)}`,
    needed_grams_display: need,
    purchase_display: buy,
    purchase_item_key: normalize(displayName),
    purchase_unit_type: "package",
  };
}

function meal(
  slot: MealFixture["slot"],
  displayName: string,
  kcal: number,
  protein_g: number,
  carbs_g: number,
  fat_g: number,
  meal_scope: MealFixture["meal_scope"],
  portion_multiplier: number,
  time_min: number,
  ingredients: string[],
): MealFixture {
  return {
    carbs_g,
    display_name: displayName,
    fat_g,
    ingredients,
    kcal,
    meal_scope,
    portion_multiplier,
    protein_g,
    recipe_id: `dima_${normalize(displayName)}`,
    slot,
    time_min,
  };
}

function day(day_index: number, meals: MealFixture[], totals: MacroTotals): MemberDayFixture {
  return { day_index, meals, totals };
}

function cleanDimaName(value: string): string {
  return value.replace(/\s+Dima$/i, "").trim() || value;
}

function normalize(value: string | undefined): string {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

const ALICE_DAYS: MemberDayFixture[] = [
  day(
    1,
    [
      meal("breakfast", "Greek Yogurt Berry Oat Bowl", 430, 28, 55, 11, "individual", 1, 10, [
        "220 g Greek yogurt",
        "45 g rolled oats",
        "120 g mixed berries",
        "1 small banana",
      ]),
      meal("lunch", "Mediterranean Chickpea and Feta Bowl", 560, 28, 74, 17, "shared", 1, 22, [
        "160 g cooked chickpeas",
        "120 g rice",
        "80 g tomatoes",
        "40 g feta cheese",
      ]),
      meal("snack", "Apple, Cottage Cheese and Walnuts", 260, 20, 25, 10, "individual", 1, 5, [
        "1 apple",
        "150 g cottage cheese",
        "15 g walnuts",
      ]),
      meal("dinner", "Tofu Vegetable Rice Bowl", 610, 32, 82, 18, "shared", 1, 30, [
        "180 g firm tofu",
        "160 g cooked rice",
        "200 g mixed vegetables",
        "1 tbsp soy sauce",
      ]),
    ],
    { carbs_g: 236, fat_g: 56, kcal: 1860, protein_g: 108 },
  ),
  day(
    2,
    [
      meal("breakfast", "Vegetable Omelette with Feta and Toast", 470, 30, 38, 22, "individual", 1, 18, [
        "2 eggs",
        "80 g vegetables",
        "30 g feta cheese",
        "1 slice wholegrain toast",
      ]),
      meal("lunch", "Lentil Tomato Pasta", 620, 34, 92, 13, "shared", 1, 28, [
        "110 g dry pasta",
        "160 g cooked lentils",
        "150 g tomato sauce",
        "80 g zucchini",
      ]),
      meal("snack", "Greek Yogurt with Banana", 250, 21, 34, 4, "individual", 1, 5, [
        "180 g Greek yogurt",
        "1 banana",
      ]),
      meal("dinner", "Tofu Broccoli Stir Fry with Rice", 580, 35, 78, 16, "shared", 1, 28, [
        "180 g firm tofu",
        "180 g broccoli",
        "150 g cooked rice",
        "1 tbsp soy sauce",
      ]),
    ],
    { carbs_g: 242, fat_g: 55, kcal: 1920, protein_g: 120 },
  ),
  day(
    3,
    [
      meal("breakfast", "Cottage Cheese Avocado Toast with Egg", 490, 32, 44, 22, "individual", 1, 14, [
        "2 slices wholegrain bread",
        "160 g cottage cheese",
        "1 egg",
        "1/2 avocado",
      ]),
      meal("lunch", "Black Bean Burrito Bowl", 610, 30, 92, 16, "shared", 1, 24, [
        "180 g black beans",
        "150 g cooked rice",
        "80 g tomatoes",
        "1 tortilla",
      ]),
      meal("snack", "Yogurt, Apple and Peanut Butter", 280, 20, 30, 11, "individual", 1, 5, [
        "160 g Greek yogurt",
        "1 apple",
        "15 g peanut butter",
      ]),
      meal("dinner", "Vegetable Lentil Pasta Bake", 620, 36, 88, 15, "shared", 1, 35, [
        "110 g dry pasta",
        "170 g cooked lentils",
        "180 g tomato sauce",
        "120 g vegetables",
      ]),
    ],
    { carbs_g: 254, fat_g: 64, kcal: 2000, protein_g: 118 },
  ),
];

const ADRIAN_DAYS: MemberDayFixture[] = [
  day(
    1,
    [
      meal("breakfast", "Greek Yogurt Berry Oat Bowl, lighter portion", 390, 30, 45, 9, "individual", 0.9, 10, [
        "220 g Greek yogurt",
        "35 g rolled oats",
        "100 g mixed berries",
      ]),
      meal("lunch", "Mediterranean Chicken and Chickpea Bowl", 610, 52, 58, 17, "shared", 0.95, 25, [
        "150 g chicken breast",
        "90 g cooked chickpeas",
        "110 g rice",
        "100 g salad vegetables",
      ]),
      meal("snack", "Apple and Cottage Cheese", 220, 24, 18, 6, "individual", 0.9, 5, [
        "1 apple",
        "180 g cottage cheese",
      ]),
      meal("dinner", "Chicken Vegetable Rice Bowl", 690, 58, 72, 16, "shared", 0.95, 30, [
        "170 g chicken breast",
        "150 g cooked rice",
        "220 g mixed vegetables",
      ]),
    ],
    { carbs_g: 193, fat_g: 48, kcal: 1910, protein_g: 164 },
  ),
  day(
    2,
    [
      meal("breakfast", "Egg White Vegetable Omelette and Toast", 420, 36, 34, 14, "individual", 0.9, 18, [
        "3 egg whites",
        "1 egg",
        "80 g vegetables",
        "1 slice wholegrain toast",
      ]),
      meal("lunch", "Chicken Tomato Pasta", 640, 58, 68, 12, "shared", 0.95, 28, [
        "160 g chicken breast",
        "90 g dry pasta",
        "150 g tomato sauce",
      ]),
      meal("snack", "Greek Yogurt with Berries", 210, 22, 22, 3, "individual", 0.9, 5, [
        "180 g Greek yogurt",
        "100 g mixed berries",
      ]),
      meal("dinner", "Chicken Broccoli Stir Fry with Rice", 680, 60, 70, 14, "shared", 0.95, 28, [
        "170 g chicken breast",
        "180 g broccoli",
        "140 g cooked rice",
      ]),
    ],
    { carbs_g: 194, fat_g: 43, kcal: 1950, protein_g: 176 },
  ),
  day(
    3,
    [
      meal("breakfast", "Cottage Cheese Toast with Egg", 430, 38, 36, 14, "individual", 0.9, 14, [
        "2 slices wholegrain bread",
        "180 g cottage cheese",
        "1 egg",
      ]),
      meal("lunch", "Chicken Black Bean Burrito Bowl", 650, 58, 70, 14, "shared", 0.95, 24, [
        "160 g chicken breast",
        "120 g black beans",
        "120 g cooked rice",
        "1 tortilla",
      ]),
      meal("snack", "Yogurt and Apple", 220, 22, 25, 3, "individual", 0.9, 5, [
        "180 g Greek yogurt",
        "1 apple",
      ]),
      meal("dinner", "Chicken Vegetable Pasta Bake", 710, 62, 74, 15, "shared", 0.95, 35, [
        "170 g chicken breast",
        "95 g dry pasta",
        "160 g tomato sauce",
        "120 g vegetables",
      ]),
    ],
    { carbs_g: 205, fat_g: 46, kcal: 2010, protein_g: 180 },
  ),
];

const MARIUS_DAYS: MemberDayFixture[] = [
  day(
    1,
    [
      meal("breakfast", "Greek Yogurt Berry Oat Bowl with Peanut Butter", 620, 42, 75, 18, "individual", 1.25, 10, [
        "280 g Greek yogurt",
        "70 g rolled oats",
        "1 banana",
        "20 g peanut butter",
      ]),
      meal("lunch", "Mediterranean Chicken and Chickpea Bowl, larger serving", 820, 65, 88, 22, "shared", 1.25, 25, [
        "210 g chicken breast",
        "120 g cooked chickpeas",
        "170 g rice",
        "120 g salad vegetables",
      ]),
      meal("snack", "Banana, Cottage Cheese and Nuts", 410, 32, 44, 12, "individual", 1.2, 5, [
        "1 banana",
        "220 g cottage cheese",
        "20 g mixed nuts",
      ]),
      meal("dinner", "Chicken Vegetable Rice Bowl, larger serving", 930, 72, 110, 23, "shared", 1.25, 30, [
        "230 g chicken breast",
        "230 g cooked rice",
        "250 g mixed vegetables",
      ]),
    ],
    { carbs_g: 317, fat_g: 75, kcal: 2780, protein_g: 211 },
  ),
  day(
    2,
    [
      meal("breakfast", "Vegetable Omelette, Toast and Extra Yogurt", 700, 52, 72, 24, "individual", 1.25, 18, [
        "3 eggs",
        "100 g vegetables",
        "2 slices wholegrain toast",
        "180 g Greek yogurt",
      ]),
      meal("lunch", "Chicken Tomato Pasta, larger serving", 900, 75, 112, 18, "shared", 1.25, 28, [
        "230 g chicken breast",
        "140 g dry pasta",
        "180 g tomato sauce",
      ]),
      meal("snack", "Greek Yogurt, Banana and Granola", 430, 30, 66, 8, "individual", 1.2, 5, [
        "220 g Greek yogurt",
        "1 banana",
        "45 g granola",
      ]),
      meal("dinner", "Chicken Broccoli Stir Fry with Rice, larger serving", 890, 78, 104, 18, "shared", 1.25, 28, [
        "240 g chicken breast",
        "220 g broccoli",
        "210 g cooked rice",
      ]),
    ],
    { carbs_g: 354, fat_g: 68, kcal: 2920, protein_g: 235 },
  ),
  day(
    3,
    [
      meal("breakfast", "Cottage Cheese Toast, Eggs and Banana", 720, 55, 82, 23, "individual", 1.25, 14, [
        "3 slices wholegrain bread",
        "240 g cottage cheese",
        "2 eggs",
        "1 banana",
      ]),
      meal("lunch", "Chicken Black Bean Burrito Bowl, larger serving", 920, 76, 118, 20, "shared", 1.25, 24, [
        "230 g chicken breast",
        "170 g black beans",
        "200 g cooked rice",
        "1 tortilla",
      ]),
      meal("snack", "Yogurt, Banana and Peanut Butter", 450, 32, 56, 12, "individual", 1.2, 5, [
        "220 g Greek yogurt",
        "1 banana",
        "20 g peanut butter",
      ]),
      meal("dinner", "Chicken Vegetable Pasta Bake, larger serving", 940, 82, 112, 20, "shared", 1.25, 35, [
        "240 g chicken breast",
        "140 g dry pasta",
        "180 g tomato sauce",
        "150 g vegetables",
      ]),
    ],
    { carbs_g: 368, fat_g: 75, kcal: 3030, protein_g: 245 },
  ),
];
