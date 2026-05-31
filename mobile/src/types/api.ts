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
  bf_profile?: string;
  [key: string]: unknown;
};

export type DemoHouseholdResponse = {
  household_id: string;
  household_name?: string;
  active_member_ids?: string[];
  members: DemoMemberProfile[];
  [key: string]: unknown;
};

export type IndividualPlanGenerateRequest = {
  dataset_profile: string;
  days: number;
  household_id?: string;
  member_profile_id?: string;
  member_profile: DemoMemberProfile;
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

export type GeneratedMeal = {
  slot?: string;
  recipe_id?: string;
  display_name?: string;
  kcal?: number;
  protein_g?: number;
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
