export type HealthResponse = {
  status: string;
  service: string;
  version: string;
  database: string;
};

export type DemoMemberProfile = {
  member_id?: string;
  member_profile_id?: string;
  display_name?: string;
  profile_name?: string;
  age?: number;
  sex?: string;
  weight_kg?: number;
  height_cm?: number;
  activity_level?: string;
  goal?: string;
  goal_speed?: string;
  training?: Record<string, unknown>;
  meal_config?: Record<string, unknown>;
  dietary_preferences?: Record<string, unknown>;
  food_preferences?: FoodPreferences;
  health_and_diet_preferences?: HealthAndDietPreferences;
  bf_profile?: string;
  [key: string]: unknown;
};

export type FoodPreferenceRating = "like" | "dislike" | "avoid";

export type FoodPreferences = {
  ratings: Record<string, FoodPreferenceRating>;
  avoid_ingredients: string[];
  cooking_time_preference: "quick" | "balanced" | "no_rush";
};

export type HealthAndDietPreferences = {
  dietary_patterns: {
    keto: boolean;
    paleo: boolean;
    mediterranean: boolean;
  };
  health_modes: {
    diabetes_aware: boolean;
    hypertension_friendly: boolean;
    heart_friendly: boolean;
  };
};

export type DemoHouseholdResponse = {
  household_id: string;
  household_name?: string;
  active_member_ids?: string[];
  members: DemoMemberProfile[];
  [key: string]: unknown;
};

export type RegisterRequest = {
  email: string;
  password: string;
  confirm_password: string;
};

export type LoginRequest = {
  email: string;
  password: string;
};

export type AuthAccount = {
  user_id: string;
  email: string;
  household_id: string;
  household_display_name: string;
};

export type AuthResponse = {
  status: string;
  message: string;
  session_token: string;
  account: AuthAccount;
};

export type MeResponse = {
  status: string;
  account: AuthAccount | null;
};

export type HouseholdSettingsResponse = {
  status: string;
  message: string;
  account: AuthAccount;
};

export type MemberProfileCreateRequest = {
  household_id?: string;
  member_profile_id?: string;
  display_name: string;
  age: number;
  sex: string;
  weight_kg: number;
  height_cm: number;
  activity_level: string;
  goal: string;
  goal_speed: string;
  training: Record<string, unknown>;
  meal_config: Record<string, unknown>;
  dietary_preferences: Record<string, unknown>;
  food_preferences?: FoodPreferences;
  health_and_diet_preferences?: HealthAndDietPreferences;
  bf_profile?: string;
  [key: string]: unknown;
};

export type MemberProfileResponse = {
  member_profile_id: string;
  household_id: string;
  display_name: string;
  age: number;
  sex: string;
  weight_kg: number;
  height_cm: number;
  activity_level: string;
  goal: string;
  goal_speed: string;
  training: Record<string, unknown>;
  meal_config: Record<string, unknown>;
  dietary_preferences: Record<string, unknown>;
  food_preferences: FoodPreferences;
  health_and_diet_preferences: HealthAndDietPreferences;
  bf_profile?: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  [key: string]: unknown;
};

export type ProfilesListResponse = {
  household_id?: string | null;
  source?: string;
  profiles: MemberProfileResponse[];
  [key: string]: unknown;
};

export type DeleteProfileResponse = {
  status: string;
  member_profile_id: string;
  deactivated: boolean;
  deleted?: boolean;
  message?: string;
};

export type IndividualPlanGenerateRequest = {
  dataset_profile: string;
  days: number;
  household_id?: string;
  member_profile_id?: string;
  member_profile?: DemoMemberProfile;
  generation_options: Record<string, unknown>;
  include_grocery_list: boolean;
  include_purchase_suggestions: boolean;
  include_price_estimates: boolean;
  feedback_enabled: boolean;
};

export type HouseholdPlanGenerateRequest = {
  dataset_profile: string;
  days: number;
  household_id?: string;
  household_profile?: DemoHouseholdResponse;
  selected_member_ids: string[];
  generation_options: Record<string, unknown>;
  include_grocery_list: boolean;
  include_purchase_suggestions: boolean;
  include_price_estimates: boolean;
  feedback_enabled: boolean;
  household_mode?: string;
  household_allocation_mode?: string;
  [key: string]: unknown;
};

export type GroceryListItem = {
  display_name?: string;
  display_name_clean?: string;
  category?: string;
  category_label?: string;
  grocery_category?: string;
  needed_grams?: number;
  needed_grams_display?: string;
  needed_grams_exact?: number;
  display_grams?: string;
  total_grams?: number;
  purchase_display?: string;
  purchase_item_key?: string;
  purchase_unit_type?: string;
  estimated_cost?: number | null;
  estimated_cost_display?: string;
  currency?: string;
  price_confidence?: string;
  price_warning?: string;
  warnings?: unknown[] | string;
  purchase_warnings?: unknown[] | string;
  source_recipes?: unknown[] | string;
  [key: string]: unknown;
};

export type GroceryListSummary = {
  estimated_total_cost?: number;
  total_estimated_cost?: number;
  priced_item_count?: number;
  missing_price_count?: number;
  unpriced_item_count?: number;
  warnings?: unknown[] | string;
  pricing_summary?: Record<string, unknown>;
  [key: string]: unknown;
};

export type GroceryListResponse = {
  status?: string;
  grocery_list_id?: string;
  plan_id?: string;
  generation_type?: string;
  currency?: string;
  estimated_total_cost?: number;
  total_estimated_cost?: number;
  priced_item_count?: number;
  missing_price_count?: number;
  items?: GroceryListItem[];
  display_items?: GroceryListItem[];
  summary?: GroceryListSummary;
  warnings?: unknown[] | string;
  [key: string]: unknown;
};

export type MealIngredientAmount = {
  text: string;
  name?: string;
  raw_text?: string;
  amount_text?: string;
  portion_multiplier?: number;
  quantity_value_scaled?: number | null;
  quantity_unit?: string;
  quantity_grams_scaled?: number | null;
  is_optional?: boolean;
};

export type GeneratedMeal = {
  slot?: string;
  recipe_id?: string;
  display_name?: string;
  kcal?: number;
  carbs_g?: number;
  fat_g?: number;
  protein_g?: number;
  cooking_steps?: string[];
  directions?: string[];
  directions_step_count?: number;
  ingredients?: string[];
  ingredient_amounts?: MealIngredientAmount[];
  warnings?: unknown[];
  [key: string]: unknown;
};

export type HouseholdMeal = GeneratedMeal & {
  carbs_g?: number;
  fat_g?: number;
  meal_scope?: string;
  portion_multiplier?: number;
};

export type GeneratedDay = {
  day_index?: number;
  selected_meals?: GeneratedMeal[];
  totals?: {
    kcal?: number;
    protein_g?: number;
    carbs_g?: number;
    fat_g?: number;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export type HouseholdMacroTotals = {
  kcal?: number;
  protein_g?: number;
  carbs_g?: number;
  fat_g?: number;
  [key: string]: unknown;
};

export type HouseholdAllocation = {
  quality?: string;
  status?: string;
  accept_count?: number;
  review_count?: number;
  reject_count?: number;
  warnings?: unknown[] | string;
  [key: string]: unknown;
};

export type HouseholdGeneratedDay = Omit<GeneratedDay, "selected_meals"> & {
  selected_meals?: HouseholdMeal[];
  meals?: HouseholdMeal[];
  shared_meals?: HouseholdMeal[];
  quality?: string;
  quality_status?: string;
  allocation?: HouseholdAllocation;
};

export type HouseholdMemberMenu = {
  member_id?: string;
  member_profile_id?: string;
  day_index?: number;
  day?: number;
  meals?: HouseholdMeal[];
  selected_meals?: HouseholdMeal[];
  totals?: HouseholdMacroTotals;
  daily_totals?: HouseholdMacroTotals;
  macro_totals?: HouseholdMacroTotals;
  [key: string]: unknown;
};

export type HouseholdMemberTarget = {
  member_id?: string;
  member_profile_id?: string;
  display_name?: string;
  profile_name?: string;
  target_kcal?: number;
  kcal?: number;
  protein_g?: number;
  carbs_g?: number;
  fat_g?: number;
  [key: string]: unknown;
};

export type HouseholdMemberMacroSummary = {
  member_id?: string;
  member_profile_id?: string;
  day_index?: number;
  day?: number;
  kcal?: number;
  protein_g?: number;
  carbs_g?: number;
  fat_g?: number;
  target_kcal?: number;
  kcal_ratio?: number;
  protein_ratio?: number;
  carbs_ratio?: number;
  fat_ratio?: number;
  ratios?: Record<string, unknown>;
  totals?: HouseholdMacroTotals;
  targets?: HouseholdMacroTotals;
  [key: string]: unknown;
};

export type IndividualPlanGenerateResponse = {
  status: string;
  plan_id?: string;
  generation_type?: string;
  member_profile_id?: string;
  days?: number;
  daily_plan?: GeneratedDay[];
  grocery_list?: GroceryListResponse | null;
  warnings?: unknown[];
  diagnostics_summary?: Record<string, unknown>;
  feedback_context_summary?: Record<string, unknown>;
  [key: string]: unknown;
};

export type HouseholdPlanGenerateResponse = {
  status: string;
  plan_id?: string;
  household_plan_id?: string;
  generation_type?: string;
  dataset_profile?: string;
  household_id?: string;
  selected_members?: DemoMemberProfile[];
  days?: number;
  daily_plan?: HouseholdGeneratedDay[];
  member_targets?: HouseholdMemberTarget[];
  per_member_menus?: HouseholdMemberMenu[];
  shared_meals?: unknown[];
  household_grocery_list?: GroceryListResponse | null;
  household_grocery_scaling?: Record<string, unknown>;
  member_macro_summaries?: HouseholdMemberMacroSummary[];
  diagnostics_summary?: Record<string, unknown>;
  feedback_context_summary?: Record<string, unknown>;
  warnings?: unknown[] | string;
  [key: string]: unknown;
};

export type FeedbackType = "liked" | "disliked" | "too_long" | "explicit_avoid";

export type FeedbackEventRequest = {
  event_id?: string;
  household_id: string;
  member_profile_id?: string;
  plan_id?: string;
  recipe_id: string;
  slot?: string;
  feedback_type: FeedbackType;
  notes?: string;
  source: "mobile";
};

export type FeedbackEventResponse = {
  event_id: string;
  status: string;
  feedback_type: FeedbackType;
  recipe_id: string;
  created_at: string;
};

export type FeedbackContextResponse = {
  household_id?: string;
  member_profile_id?: string;
  event_count: number;
  hard_filters?: Record<string, unknown>;
  score_preferences?: Record<string, unknown>;
  time_preferences?: Record<string, unknown>;
  meta?: Record<string, unknown>;
  [key: string]: unknown;
};

export type FeedbackDeleteResponse = {
  deleted: boolean;
  deleted_event_count: number;
  household_id?: string;
  member_profile_id?: string;
};

export type RecipeAlternativesApprovalMode =
  | "approved_only"
  | "include_review"
  | "include_rejected_debug";

export type RecipeAlternativesRequest = {
  recipe_id: string;
  slot?: string;
  top_k?: number;
  candidate_pool_k?: number;
  dataset_profile?: string;
  member_profile_id?: string;
  member_profile?: Record<string, unknown>;
  household_id?: string;
  feedback_enabled?: boolean;
  approval_mode?: RecipeAlternativesApprovalMode;
  generation_options?: Record<string, unknown>;
  [key: string]: unknown;
};

export type RecipeAlternativeItem = {
  recipe_id: string;
  display_name?: string;
  similarity_score?: number | null;
  approval_status?: "approved" | "review" | "rejected" | string;
  approval_reasons?: string[];
  rejection_reasons?: string[];
  macro_delta?: Record<string, unknown>;
  time_delta_min?: number | null;
  why_similar?: string[] | string;
  warnings?: string[] | string;
  diagnostics?: Record<string, unknown>;
  [key: string]: unknown;
};

export type RecipeAlternativesResponse = {
  status: string;
  recipe_id: string;
  source_recipe?: {
    recipe_id?: string;
    display_name?: string;
    [key: string]: unknown;
  };
  slot?: string | null;
  dataset_profile?: string;
  approval_mode?: RecipeAlternativesApprovalMode | string;
  alternatives?: RecipeAlternativeItem[];
  summary?: Record<string, unknown>;
  feedback_context_summary?: Record<string, unknown>;
  warnings?: string[] | string;
  [key: string]: unknown;
};

export type MealReplacementScope =
  | "individual_meal"
  | "household_member_meal"
  | "household_shared_meal";

export type MealReplacementRequest = {
  day_index: number;
  slot: string;
  current_recipe_id: string;
  alternative_recipe_id: string;
  generation_type?: "individual" | "household" | string;
  replace_scope?: MealReplacementScope;
  member_id?: string;
  member_profile_id?: string;
  dataset_profile?: string;
  feedback_enabled?: boolean;
  generation_options?: Record<string, unknown>;
  [key: string]: unknown;
};

export type MealReplacementImpact = {
  meal_macro_delta?: Record<string, unknown>;
  day_totals_before?: Record<string, unknown>;
  day_totals_after?: Record<string, unknown>;
  day_totals_delta?: Record<string, unknown>;
  affected_members?: string[];
  grocery_rebuilt?: boolean;
  grocery_item_count?: number | null;
  warnings?: unknown[] | string;
  [key: string]: unknown;
};

export type MealReplacementResponse = {
  status: string;
  dry_run: boolean;
  replacement_allowed?: boolean;
  approval_status?: string | null;
  plan_id?: string | null;
  source_plan_id?: string | null;
  new_plan_id?: string | null;
  generation_type?: "individual" | "household" | string;
  replacement?: {
    current_meal?: GeneratedMeal;
    alternative_meal?: GeneratedMeal;
    replace_scope?: MealReplacementScope | string;
    [key: string]: unknown;
  };
  impact?: MealReplacementImpact;
  updated_plan?: IndividualPlanGenerateResponse | HouseholdPlanGenerateResponse | Record<string, unknown>;
  grocery_list?: GroceryListResponse | null;
  warnings?: unknown[] | string;
  [key: string]: unknown;
};
