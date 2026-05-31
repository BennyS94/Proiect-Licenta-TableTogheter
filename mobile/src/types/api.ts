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
  member_profile: DemoMemberProfile;
  generation_options: Record<string, unknown>;
  include_grocery_list: boolean;
  include_purchase_suggestions: boolean;
  include_price_estimates: boolean;
  feedback_enabled: boolean;
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
  warnings?: unknown[];
  diagnostics_summary?: Record<string, unknown>;
  feedback_context_summary?: Record<string, unknown>;
  [key: string]: unknown;
};
