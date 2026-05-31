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
