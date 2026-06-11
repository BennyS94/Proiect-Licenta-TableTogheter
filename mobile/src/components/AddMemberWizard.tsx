import { useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import { colors } from "../theme/colors";
import type {
  HealthAndDietPreferences,
  MemberProfileCreateRequest,
} from "../types/api";

type AddMemberWizardProps = {
  defaultHouseholdId: string;
  disabled?: boolean;
  onSubmit: (request: MemberProfileCreateRequest) => Promise<void> | void;
};

type WizardStep = 1 | 2 | 3;
type PreferenceRating = "like" | "dislike" | "avoid";
type CookingTimePreference = "quick" | "balanced" | "no_rush";
type DietaryDraft = {
  vegetarian: boolean;
  vegan: boolean;
  gluten_free: boolean;
};
type DietaryPatternKey = keyof HealthAndDietPreferences["dietary_patterns"];
type DietaryPatternDraft = HealthAndDietPreferences["dietary_patterns"];
type HealthModeKey = keyof HealthAndDietPreferences["health_modes"];
type HealthModeDraft = HealthAndDietPreferences["health_modes"];

type FoodPreferenceItem = {
  key: string;
  label: string;
};

type FoodPreferenceSection = {
  title: string;
  items: FoodPreferenceItem[];
};

const SEX_OPTIONS = [
  { label: "Male", value: "male" },
  { label: "Female", value: "female" },
];

const GOAL_OPTIONS = [
  { label: "Lose weight", value: "lose" },
  { label: "Maintain", value: "maintain" },
  { label: "Gain muscle", value: "gain" },
  { label: "Balanced eating", value: "balanced" },
];

const GOAL_SPEED_OPTIONS = [
  { label: "Slow", value: "slow" },
  { label: "Normal", value: "normal" },
  { label: "Fast", value: "fast" },
];

const ACTIVITY_OPTIONS = [
  { label: "Sedentary", value: "sedentary" },
  { label: "Lightly active", value: "lightly_active" },
  { label: "Moderately active", value: "moderately_active" },
  { label: "Very active", value: "very_active" },
];

const TRAINING_TYPE_OPTIONS = [
  { label: "None", value: "none" },
  { label: "Weights", value: "weights" },
  { label: "Cardio", value: "cardio" },
  { label: "Mixed", value: "mixed" },
];

const COOKING_TIME_OPTIONS: Array<{ label: string; value: CookingTimePreference }> = [
  { label: "Quick", value: "quick" },
  { label: "Balanced", value: "balanced" },
  { label: "No rush", value: "no_rush" },
];

const DIETARY_PATTERN_OPTIONS: Array<{ label: string; value: DietaryPatternKey }> = [
  { label: "Keto", value: "keto" },
  { label: "Paleo", value: "paleo" },
  { label: "Mediterranean", value: "mediterranean" },
];

const HEALTH_MODE_OPTIONS: Array<{ label: string; value: HealthModeKey }> = [
  { label: "Diabetes-aware", value: "diabetes_aware" },
  { label: "Blood-pressure friendly", value: "hypertension_friendly" },
  { label: "Heart-friendly", value: "heart_friendly" },
];

const FOOD_SECTIONS: FoodPreferenceSection[] = [
  {
    title: "Protein sources",
    items: [
      { key: "chicken", label: "Chicken" },
      { key: "turkey", label: "Turkey" },
      { key: "beef", label: "Beef" },
      { key: "pork", label: "Pork" },
      { key: "fish", label: "Fish" },
      { key: "eggs", label: "Eggs" },
      { key: "dairy", label: "Dairy" },
    ],
  },
  {
    title: "Carbs & staples",
    items: [
      { key: "rice", label: "Rice" },
      { key: "pasta", label: "Pasta" },
      { key: "potatoes", label: "Potatoes" },
      { key: "oats", label: "Oats" },
      { key: "bread", label: "Bread" },
    ],
  },
  {
    title: "Other foods",
    items: [
      { key: "beans", label: "Beans" },
      { key: "vegetables", label: "Vegetables" },
      { key: "soups", label: "Soups" },
      { key: "spicy_food", label: "Spicy food" },
      { key: "mushrooms", label: "Mushrooms" },
      { key: "onions", label: "Onions" },
    ],
  },
];

const VEGETARIAN_AVOID_KEYS = ["beef", "pork", "chicken", "turkey", "fish"];
const VEGAN_AVOID_KEYS = [...VEGETARIAN_AVOID_KEYS, "eggs", "dairy"];

const DEFAULT_DIETARY: DietaryDraft = {
  vegetarian: false,
  vegan: false,
  gluten_free: false,
};

const DEFAULT_DIETARY_PATTERNS: DietaryPatternDraft = {
  keto: false,
  paleo: false,
  mediterranean: false,
};

const DEFAULT_HEALTH_MODES: HealthModeDraft = {
  diabetes_aware: false,
  hypertension_friendly: false,
  heart_friendly: false,
};

export function AddMemberWizard({
  defaultHouseholdId,
  disabled,
  onSubmit,
}: AddMemberWizardProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [step, setStep] = useState<WizardStep>(1);
  const [displayName, setDisplayName] = useState("");
  const [sex, setSex] = useState("male");
  const [age, setAge] = useState("30");
  const [heightCm, setHeightCm] = useState("175");
  const [weightKg, setWeightKg] = useState("75");
  const [dietary, setDietary] = useState<DietaryDraft>(DEFAULT_DIETARY);
  const [dietaryPatterns, setDietaryPatterns] =
    useState<DietaryPatternDraft>(DEFAULT_DIETARY_PATTERNS);
  const [healthModes, setHealthModes] =
    useState<HealthModeDraft>(DEFAULT_HEALTH_MODES);
  const [ratings, setRatings] = useState<Record<string, PreferenceRating>>({});
  const [customAvoidInput, setCustomAvoidInput] = useState("");
  const [avoidIngredients, setAvoidIngredients] = useState<string[]>([]);
  const [goal, setGoal] = useState("maintain");
  const [goalSpeed, setGoalSpeed] = useState("normal");
  const [activityLevel, setActivityLevel] = useState("moderately_active");
  const [trainingType, setTrainingType] = useState("mixed");
  const [trainingSessions, setTrainingSessions] = useState("3");
  const [mealsPerDay, setMealsPerDay] = useState("3");
  const [includeSnacks, setIncludeSnacks] = useState(true);
  const [cookingTimePreference, setCookingTimePreference] =
    useState<CookingTimePreference>("balanced");
  const [validationError, setValidationError] = useState("");

  function resetDraft() {
    setStep(1);
    setDisplayName("");
    setSex("male");
    setAge("30");
    setHeightCm("175");
    setWeightKg("75");
    setDietary(DEFAULT_DIETARY);
    setDietaryPatterns(DEFAULT_DIETARY_PATTERNS);
    setHealthModes(DEFAULT_HEALTH_MODES);
    setRatings({});
    setCustomAvoidInput("");
    setAvoidIngredients([]);
    setGoal("maintain");
    setGoalSpeed("normal");
    setActivityLevel("moderately_active");
    setTrainingType("mixed");
    setTrainingSessions("3");
    setMealsPerDay("3");
    setIncludeSnacks(true);
    setCookingTimePreference("balanced");
    setValidationError("");
  }

  function validateCurrentStep(): boolean {
    const cleanName = displayName.trim();
    if (step === 1) {
      if (!cleanName) {
        setValidationError("Name is required.");
        return false;
      }
      if (/\d/.test(cleanName)) {
        setValidationError("Name cannot contain numbers.");
        return false;
      }
      if (!isNumberInRange(Number(age), 4, 120)) {
        setValidationError("Age must be between 4 and 120.");
        return false;
      }
      if (!isNumberInRange(Number(heightCm), 80, 230)) {
        setValidationError("Height must be between 80 and 230 cm.");
        return false;
      }
      if (!isNumberInRange(Number(weightKg), 15, 300)) {
        setValidationError("Weight must be between 15 and 300 kg.");
        return false;
      }
    }
    if (step === 3) {
      if (!isNumberInRange(Number(trainingSessions), 0, 7)) {
        setValidationError("Training sessions must be between 0 and 7.");
        return false;
      }
      if (!isNumberInRange(Number(mealsPerDay), 1, 5)) {
        setValidationError("Meals per day must be between 1 and 5.");
        return false;
      }
    }
    setValidationError("");
    return true;
  }

  function goNext() {
    if (!validateCurrentStep()) {
      return;
    }
    setStep((current) => (current < 3 ? ((current + 1) as WizardStep) : current));
  }

  function goBack() {
    setValidationError("");
    setStep((current) => (current > 1 ? ((current - 1) as WizardStep) : current));
  }

  async function saveMember() {
    if (!validateCurrentStep()) {
      return;
    }
    const request = buildProfileRequest();
    try {
      await onSubmit(request);
      resetDraft();
      setIsOpen(false);
    } catch (error) {
      setValidationError(error instanceof Error ? error.message : "Member save failed.");
    }
  }

  function buildProfileRequest(): MemberProfileCreateRequest {
    const cleanRatings = cleanRatingMap(ratings);
    const dietaryPreferences = buildDietaryPreferences(dietary, cleanRatings);
    const backendGoal = goal === "balanced" ? "maintain" : goal;
    return {
      household_id: defaultHouseholdId.trim(),
      display_name: displayName.trim(),
      age: Math.round(Number(age)),
      sex,
      weight_kg: Number(weightKg),
      height_cm: Number(heightCm),
      activity_level: activityLevel,
      goal: backendGoal,
      goal_speed: backendGoal === "maintain" ? "normal" : goalSpeed,
      training: {
        sessions_per_week: Math.round(Number(trainingSessions)),
        type: trainingType,
      },
      meal_config: {
        meals_per_day: Math.round(Number(mealsPerDay)),
        include_snacks: includeSnacks,
        day_structure: includeSnacks ? "3_meals_plus_snack" : "3_meals",
      },
      dietary_preferences: dietaryPreferences,
      food_preferences: {
        ratings: cleanRatings,
        avoid_ingredients: avoidIngredients,
        cooking_time_preference: cookingTimePreference,
      },
      health_and_diet_preferences: {
        dietary_patterns: dietaryPatterns,
        health_modes: healthModes,
      },
      bf_profile: "normal",
    };
  }

  function toggleDietary(key: keyof DietaryDraft) {
    setDietary((current) => {
      const next = { ...current, [key]: !current[key] };
      if (key === "vegan" && next.vegan) {
        next.vegetarian = true;
        setRatings((currentRatings) => forceAvoidRatings(currentRatings, VEGAN_AVOID_KEYS));
      } else if (key === "vegetarian" && next.vegetarian) {
        setRatings((currentRatings) => forceAvoidRatings(currentRatings, VEGETARIAN_AVOID_KEYS));
      }
      return next;
    });
  }

  function toggleRating(foodKey: string, rating: PreferenceRating) {
    setRatings((current) => {
      if (current[foodKey] === rating) {
        const next = { ...current };
        delete next[foodKey];
        return next;
      }
      return { ...current, [foodKey]: rating };
    });
  }

  function toggleDietaryPattern(key: DietaryPatternKey) {
    setDietaryPatterns((current) => ({
      ...current,
      [key]: !current[key],
    }));
  }

  function toggleHealthMode(key: HealthModeKey) {
    setHealthModes((current) => ({
      ...current,
      [key]: !current[key],
    }));
  }

  function addAvoidIngredient() {
    const cleanValue = customAvoidInput.trim();
    if (!cleanValue) {
      return;
    }
    setAvoidIngredients((current) =>
      current.some((item) => item.toLowerCase() === cleanValue.toLowerCase())
        ? current
        : [...current, cleanValue],
    );
    setCustomAvoidInput("");
  }

  function removeAvoidIngredient(value: string) {
    setAvoidIngredients((current) => current.filter((item) => item !== value));
  }

  if (!isOpen) {
    return (
      <View style={styles.closedState}>
        <Pressable
          accessibilityRole="button"
          disabled={disabled}
          onPress={() => setIsOpen(true)}
          style={({ pressed }) => [
            styles.primaryButton,
            pressed && !disabled ? styles.pressed : null,
            disabled ? styles.disabled : null,
          ]}
        >
          <Text style={styles.primaryButtonText}>Add Member</Text>
        </Pressable>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <View style={styles.titleBlock}>
          <Text style={styles.title}>Add Member</Text>
          <Text style={styles.progress}>Step {step} of 3</Text>
        </View>
        <Pressable
          accessibilityRole="button"
          onPress={() => {
            resetDraft();
            setIsOpen(false);
          }}
          style={({ pressed }) => [styles.closeButton, pressed ? styles.pressed : null]}
        >
          <Text style={styles.closeText}>Cancel</Text>
        </Pressable>
      </View>

      {step === 1 ? (
        <GeneralInfoStep
          age={age}
          displayName={displayName}
          heightCm={heightCm}
          sex={sex}
          weightKg={weightKg}
          onAgeChange={setAge}
          onDisplayNameChange={setDisplayName}
          onHeightChange={setHeightCm}
          onSexChange={setSex}
          onWeightChange={setWeightKg}
        />
      ) : null}

      {step === 2 ? (
        <FoodPreferencesStep
          avoidIngredients={avoidIngredients}
          customAvoidInput={customAvoidInput}
          dietary={dietary}
          dietaryPatterns={dietaryPatterns}
          healthModes={healthModes}
          ratings={ratings}
          onAddAvoidIngredient={addAvoidIngredient}
          onCustomAvoidInputChange={setCustomAvoidInput}
          onRemoveAvoidIngredient={removeAvoidIngredient}
          onToggleDietary={toggleDietary}
          onToggleDietaryPattern={toggleDietaryPattern}
          onToggleHealthMode={toggleHealthMode}
          onToggleRating={toggleRating}
        />
      ) : null}

      {step === 3 ? (
        <ActivityGoalStep
          activityLevel={activityLevel}
          cookingTimePreference={cookingTimePreference}
          goal={goal}
          goalSpeed={goalSpeed}
          includeSnacks={includeSnacks}
          mealsPerDay={mealsPerDay}
          trainingSessions={trainingSessions}
          trainingType={trainingType}
          onActivityLevelChange={setActivityLevel}
          onCookingTimePreferenceChange={setCookingTimePreference}
          onGoalChange={(value) => {
            setGoal(value);
            if (value === "maintain" || value === "balanced") {
              setGoalSpeed("normal");
            }
          }}
          onGoalSpeedChange={setGoalSpeed}
          onIncludeSnacksChange={setIncludeSnacks}
          onMealsPerDayChange={setMealsPerDay}
          onTrainingSessionsChange={setTrainingSessions}
          onTrainingTypeChange={setTrainingType}
        />
      ) : null}

      {validationError ? <Text style={styles.errorText}>{validationError}</Text> : null}

      <View style={styles.footer}>
        <Pressable
          accessibilityRole="button"
          disabled={step === 1 || disabled}
          onPress={goBack}
          style={({ pressed }) => [
            styles.secondaryButton,
            step === 1 || disabled ? styles.disabled : null,
            pressed && step !== 1 && !disabled ? styles.pressed : null,
          ]}
        >
          <Text style={styles.secondaryButtonText}>Back</Text>
        </Pressable>
        {step < 3 ? (
          <Pressable
            accessibilityRole="button"
            disabled={disabled}
            onPress={goNext}
            style={({ pressed }) => [
              styles.primaryButton,
              disabled ? styles.disabled : null,
              pressed && !disabled ? styles.pressed : null,
            ]}
          >
            <Text style={styles.primaryButtonText}>Next</Text>
          </Pressable>
        ) : (
          <Pressable
            accessibilityRole="button"
            disabled={disabled}
            onPress={saveMember}
            style={({ pressed }) => [
              styles.primaryButton,
              disabled ? styles.disabled : null,
              pressed && !disabled ? styles.pressed : null,
            ]}
          >
            {disabled ? (
              <ActivityIndicator color="#FFFFFF" />
            ) : (
              <Text style={styles.primaryButtonText}>Save Member</Text>
            )}
          </Pressable>
        )}
      </View>
    </View>
  );
}

function GeneralInfoStep({
  age,
  displayName,
  heightCm,
  sex,
  weightKg,
  onAgeChange,
  onDisplayNameChange,
  onHeightChange,
  onSexChange,
  onWeightChange,
}: {
  age: string;
  displayName: string;
  heightCm: string;
  sex: string;
  weightKg: string;
  onAgeChange: (value: string) => void;
  onDisplayNameChange: (value: string) => void;
  onHeightChange: (value: string) => void;
  onSexChange: (value: string) => void;
  onWeightChange: (value: string) => void;
}) {
  return (
    <View style={styles.step}>
      <StepIntro
        subtitle="Tell us about this household member."
        title="General Info"
      />
      <TextField
        label="Name"
        onChangeText={onDisplayNameChange}
        placeholder="Alex"
        value={displayName}
      />
      <SegmentedControl
        label="Sex"
        onSelect={onSexChange}
        options={SEX_OPTIONS}
        selected={sex}
      />
      <View style={styles.twoColumns}>
        <TextField
          keyboardType="numeric"
          label="Age"
          onChangeText={onAgeChange}
          value={age}
        />
        <TextField
          keyboardType="numeric"
          label="Weight"
          onChangeText={onWeightChange}
          suffix="kg"
          value={weightKg}
        />
      </View>
      <TextField
        keyboardType="numeric"
        label="Height"
        onChangeText={onHeightChange}
        suffix="cm"
        value={heightCm}
      />
    </View>
  );
}

function FoodPreferencesStep({
  avoidIngredients,
  customAvoidInput,
  dietary,
  dietaryPatterns,
  healthModes,
  ratings,
  onAddAvoidIngredient,
  onCustomAvoidInputChange,
  onRemoveAvoidIngredient,
  onToggleDietary,
  onToggleDietaryPattern,
  onToggleHealthMode,
  onToggleRating,
}: {
  avoidIngredients: string[];
  customAvoidInput: string;
  dietary: DietaryDraft;
  dietaryPatterns: DietaryPatternDraft;
  healthModes: HealthModeDraft;
  ratings: Record<string, PreferenceRating>;
  onAddAvoidIngredient: () => void;
  onCustomAvoidInputChange: (value: string) => void;
  onRemoveAvoidIngredient: (value: string) => void;
  onToggleDietary: (key: keyof DietaryDraft) => void;
  onToggleDietaryPattern: (key: DietaryPatternKey) => void;
  onToggleHealthMode: (key: HealthModeKey) => void;
  onToggleRating: (foodKey: string, rating: PreferenceRating) => void;
}) {
  return (
    <View style={styles.step}>
      <StepIntro
        subtitle="Choose what this member likes, dislikes or wants to avoid. You can change this later."
        title="Food Preferences"
      />
      <View style={styles.subsection}>
        <Text style={styles.subsectionTitle}>Dietary restrictions</Text>
        <View style={styles.optionGrid}>
          <TogglePill
            label="Vegetarian"
            onPress={() => onToggleDietary("vegetarian")}
            selected={dietary.vegetarian}
          />
          <TogglePill
            label="Vegan"
            onPress={() => onToggleDietary("vegan")}
            selected={dietary.vegan}
          />
          <TogglePill
            label="Gluten free"
            onPress={() => onToggleDietary("gluten_free")}
            selected={dietary.gluten_free}
          />
        </View>
      </View>

      <View style={styles.subsection}>
        <Text style={styles.subsectionTitle}>Dietary patterns</Text>
        <Text style={styles.helperText}>
          These options help TableTogether prioritize and filter meals. They are not
          medical advice.
        </Text>
        <View style={styles.optionGrid}>
          {DIETARY_PATTERN_OPTIONS.map((option) => (
            <TogglePill
              key={option.value}
              label={option.label}
              onPress={() => onToggleDietaryPattern(option.value)}
              selected={dietaryPatterns[option.value]}
            />
          ))}
        </View>
      </View>

      <View style={styles.subsection}>
        <Text style={styles.subsectionTitle}>Health-aware preferences</Text>
        <View style={styles.optionGrid}>
          {HEALTH_MODE_OPTIONS.map((option) => (
            <TogglePill
              key={option.value}
              label={option.label}
              onPress={() => onToggleHealthMode(option.value)}
              selected={healthModes[option.value]}
            />
          ))}
        </View>
      </View>

      {FOOD_SECTIONS.map((section) => (
        <View key={section.title} style={styles.subsection}>
          <Text style={styles.subsectionTitle}>{section.title}</Text>
          <View style={styles.preferenceRows}>
            {section.items.map((item) => (
              <PreferenceRow
                key={item.key}
                foodKey={item.key}
                label={item.label}
                rating={ratings[item.key]}
                onToggle={onToggleRating}
              />
            ))}
          </View>
        </View>
      ))}

      <View style={styles.subsection}>
        <Text style={styles.subsectionTitle}>Avoid something else?</Text>
        <View style={styles.customAvoidRow}>
          <TextInput
            onChangeText={onCustomAvoidInputChange}
            placeholder="Add ingredient"
            style={styles.customAvoidInput}
            value={customAvoidInput}
          />
          <Pressable
            accessibilityRole="button"
            onPress={onAddAvoidIngredient}
            style={({ pressed }) => [styles.smallButton, pressed ? styles.pressed : null]}
          >
            <Text style={styles.smallButtonText}>Add</Text>
          </Pressable>
        </View>
        {avoidIngredients.length ? (
          <View style={styles.chipRow}>
            {avoidIngredients.map((ingredient) => (
              <Pressable
                accessibilityRole="button"
                key={ingredient}
                onPress={() => onRemoveAvoidIngredient(ingredient)}
                style={({ pressed }) => [styles.chip, pressed ? styles.pressed : null]}
              >
                <Text style={styles.chipText}>{ingredient}</Text>
              </Pressable>
            ))}
          </View>
        ) : null}
      </View>
    </View>
  );
}

function ActivityGoalStep({
  activityLevel,
  cookingTimePreference,
  goal,
  goalSpeed,
  includeSnacks,
  mealsPerDay,
  trainingSessions,
  trainingType,
  onActivityLevelChange,
  onCookingTimePreferenceChange,
  onGoalChange,
  onGoalSpeedChange,
  onIncludeSnacksChange,
  onMealsPerDayChange,
  onTrainingSessionsChange,
  onTrainingTypeChange,
}: {
  activityLevel: string;
  cookingTimePreference: CookingTimePreference;
  goal: string;
  goalSpeed: string;
  includeSnacks: boolean;
  mealsPerDay: string;
  trainingSessions: string;
  trainingType: string;
  onActivityLevelChange: (value: string) => void;
  onCookingTimePreferenceChange: (value: CookingTimePreference) => void;
  onGoalChange: (value: string) => void;
  onGoalSpeedChange: (value: string) => void;
  onIncludeSnacksChange: (value: boolean) => void;
  onMealsPerDayChange: (value: string) => void;
  onTrainingSessionsChange: (value: string) => void;
  onTrainingTypeChange: (value: string) => void;
}) {
  const goalSpeedDisabled = goal === "maintain" || goal === "balanced";
  return (
    <View style={styles.step}>
      <StepIntro
        subtitle="Set the nutrition goal and daily routine for this member."
        title="Activity & Goal"
      />
      <SegmentedControl
        label="Goal"
        onSelect={onGoalChange}
        options={GOAL_OPTIONS}
        selected={goal}
      />
      {!goalSpeedDisabled ? (
        <SegmentedControl
          label="Goal speed"
          onSelect={onGoalSpeedChange}
          options={GOAL_SPEED_OPTIONS}
          selected={goalSpeed}
        />
      ) : null}
      <SegmentedControl
        label="Activity level"
        onSelect={onActivityLevelChange}
        options={ACTIVITY_OPTIONS}
        selected={activityLevel}
      />
      <SegmentedControl
        label="Training type"
        onSelect={onTrainingTypeChange}
        options={TRAINING_TYPE_OPTIONS}
        selected={trainingType}
      />
      <View style={styles.twoColumns}>
        <TextField
          keyboardType="numeric"
          label="Sessions/week"
          onChangeText={onTrainingSessionsChange}
          value={trainingSessions}
        />
        <TextField
          keyboardType="numeric"
          label="Meals/day"
          onChangeText={onMealsPerDayChange}
          value={mealsPerDay}
        />
      </View>
      <View style={styles.subsection}>
        <Text style={styles.fieldLabel}>Include snack</Text>
        <View style={styles.optionGrid}>
          <TogglePill
            label="Yes"
            onPress={() => onIncludeSnacksChange(true)}
            selected={includeSnacks}
          />
          <TogglePill
            label="No"
            onPress={() => onIncludeSnacksChange(false)}
            selected={!includeSnacks}
          />
        </View>
      </View>
      <SegmentedControl
        label="Cooking time preference"
        onSelect={(value) => onCookingTimePreferenceChange(value as CookingTimePreference)}
        options={COOKING_TIME_OPTIONS}
        selected={cookingTimePreference}
      />
    </View>
  );
}

function StepIntro({ subtitle, title }: { subtitle: string; title: string }) {
  return (
    <View style={styles.stepIntro}>
      <Text style={styles.stepTitle}>{title}</Text>
      <Text style={styles.stepSubtitle}>{subtitle}</Text>
    </View>
  );
}

function TextField({
  keyboardType,
  label,
  onChangeText,
  placeholder,
  suffix,
  value,
}: {
  keyboardType?: "default" | "numeric";
  label: string;
  onChangeText: (value: string) => void;
  placeholder?: string;
  suffix?: string;
  value: string;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      <View style={styles.inputWrap}>
        <TextInput
          keyboardType={keyboardType ?? "default"}
          onChangeText={onChangeText}
          placeholder={placeholder}
          style={styles.input}
          value={value}
        />
        {suffix ? <Text style={styles.inputSuffix}>{suffix}</Text> : null}
      </View>
    </View>
  );
}

function SegmentedControl({
  label,
  onSelect,
  options,
  selected,
}: {
  label: string;
  onSelect: (value: string) => void;
  options: Array<{ label: string; value: string }>;
  selected: string;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      <View style={styles.optionGrid}>
        {options.map((option) => (
          <TogglePill
            key={option.value}
            label={option.label}
            onPress={() => onSelect(option.value)}
            selected={selected === option.value}
          />
        ))}
      </View>
    </View>
  );
}

function TogglePill({
  label,
  onPress,
  selected,
}: {
  label: string;
  onPress: () => void;
  selected: boolean;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [
        styles.pill,
        selected ? styles.pillSelected : null,
        pressed ? styles.pressed : null,
      ]}
    >
      <Text style={[styles.pillText, selected ? styles.pillTextSelected : null]}>
        {label}
      </Text>
    </Pressable>
  );
}

function PreferenceRow({
  foodKey,
  label,
  rating,
  onToggle,
}: {
  foodKey: string;
  label: string;
  rating?: PreferenceRating;
  onToggle: (foodKey: string, rating: PreferenceRating) => void;
}) {
  return (
    <View style={styles.preferenceRow}>
      <Text style={styles.preferenceLabel}>{label}</Text>
      <View style={styles.preferenceActions}>
        <RatingButton
          label="Like"
          rating="like"
          selected={rating === "like"}
          onPress={() => onToggle(foodKey, "like")}
        />
        <RatingButton
          label="Dislike"
          rating="dislike"
          selected={rating === "dislike"}
          onPress={() => onToggle(foodKey, "dislike")}
        />
        <RatingButton
          label="Avoid"
          rating="avoid"
          selected={rating === "avoid"}
          onPress={() => onToggle(foodKey, "avoid")}
        />
      </View>
    </View>
  );
}

function RatingButton({
  label,
  onPress,
  rating,
  selected,
}: {
  label: string;
  onPress: () => void;
  rating: PreferenceRating;
  selected: boolean;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [
        styles.ratingButton,
        rating === "like" ? styles.likeButton : null,
        rating === "dislike" ? styles.dislikeButton : null,
        rating === "avoid" ? styles.avoidButton : null,
        selected ? styles.ratingButtonSelected : null,
        selected && rating === "like" ? styles.likeButtonSelected : null,
        selected && rating === "dislike" ? styles.dislikeButtonSelected : null,
        selected && rating === "avoid" ? styles.avoidButtonSelected : null,
        pressed ? styles.pressed : null,
      ]}
    >
      <Text
        style={[
          styles.ratingButtonText,
          selected ? styles.ratingButtonTextSelected : null,
        ]}
      >
        {label}
      </Text>
    </Pressable>
  );
}

function buildDietaryPreferences(
  dietary: DietaryDraft,
  ratings: Record<string, PreferenceRating>,
) {
  const result = {
    vegetarian: Boolean(dietary.vegetarian || dietary.vegan),
    vegan: Boolean(dietary.vegan),
    gluten_free: Boolean(dietary.gluten_free),
    no_beef: ratings.beef === "avoid",
    no_pork: ratings.pork === "avoid",
    no_chicken: ratings.chicken === "avoid",
    no_fish: ratings.fish === "avoid",
    no_dairy: ratings.dairy === "avoid",
  };
  if (result.vegetarian) {
    result.no_beef = true;
    result.no_pork = true;
    result.no_chicken = true;
    result.no_fish = true;
  }
  if (result.vegan) {
    result.no_dairy = true;
  }
  return result;
}

function cleanRatingMap(
  ratings: Record<string, PreferenceRating>,
): Record<string, PreferenceRating> {
  return Object.fromEntries(
    Object.entries(ratings).filter(([, rating]) =>
      ["like", "dislike", "avoid"].includes(rating),
    ),
  ) as Record<string, PreferenceRating>;
}

function forceAvoidRatings(
  ratings: Record<string, PreferenceRating>,
  keys: string[],
): Record<string, PreferenceRating> {
  return {
    ...ratings,
    ...Object.fromEntries(keys.map((key) => [key, "avoid" as PreferenceRating])),
  };
}

function isNumberInRange(value: number, minimum: number, maximum: number): boolean {
  return Number.isFinite(value) && value >= minimum && value <= maximum;
}

const styles = StyleSheet.create({
  avoidButton: {
    borderColor: "#C2410C",
  },
  avoidButtonSelected: {
    backgroundColor: "#B42318",
    borderColor: "#B42318",
  },
  chip: {
    backgroundColor: "#F7FAF2",
    borderColor: colors.border,
    borderRadius: 999,
    borderWidth: 1,
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  chipRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  chipText: {
    color: colors.text,
    fontSize: 13,
    fontWeight: "800",
  },
  closeButton: {
    paddingHorizontal: 4,
    paddingVertical: 4,
  },
  closeText: {
    color: colors.mutedSoft,
    fontSize: 14,
    fontWeight: "800",
  },
  closedState: {
    gap: 10,
  },
  container: {
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 14,
    padding: 14,
  },
  customAvoidRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  customAvoidInput: {
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    color: colors.text,
    flex: 1,
    fontSize: 15,
    minHeight: 44,
    paddingHorizontal: 12,
  },
  disabled: {
    opacity: 0.55,
  },
  dislikeButton: {
    borderColor: "#B7791F",
  },
  dislikeButtonSelected: {
    backgroundColor: "#B7791F",
    borderColor: "#B7791F",
  },
  errorText: {
    color: colors.danger,
    fontSize: 14,
    fontWeight: "800",
  },
  field: {
    flex: 1,
    gap: 7,
  },
  fieldLabel: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "900",
    textTransform: "uppercase",
  },
  footer: {
    flexDirection: "row",
    gap: 10,
  },
  header: {
    alignItems: "flex-start",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  helperText: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
    lineHeight: 18,
  },
  input: {
    color: colors.text,
    flex: 1,
    fontSize: 15,
    minHeight: 44,
    paddingHorizontal: 12,
    textAlign: "center",
  },
  inputSuffix: {
    color: colors.mutedSoft,
    fontSize: 13,
    fontWeight: "900",
    paddingRight: 12,
  },
  inputWrap: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    minHeight: 46,
  },
  likeButton: {
    borderColor: colors.accent,
  },
  likeButtonSelected: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  optionGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  pill: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    minHeight: 40,
    paddingHorizontal: 12,
    paddingVertical: 9,
  },
  pillSelected: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  pillText: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
  },
  pillTextSelected: {
    color: "#FFFFFF",
  },
  preferenceActions: {
    flexDirection: "row",
    gap: 6,
  },
  preferenceLabel: {
    color: colors.text,
    flex: 1,
    fontSize: 15,
    fontWeight: "900",
  },
  preferenceRow: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 10,
    minHeight: 54,
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  preferenceRows: {
    gap: 8,
  },
  pressed: {
    opacity: 0.82,
  },
  primaryButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    flex: 1,
    justifyContent: "center",
    minHeight: 48,
    paddingHorizontal: 14,
  },
  primaryButtonText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "900",
  },
  progress: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "900",
  },
  ratingButton: {
    alignItems: "center",
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 34,
    minWidth: 62,
    paddingHorizontal: 7,
  },
  ratingButtonSelected: {
    borderWidth: 1,
  },
  ratingButtonText: {
    color: colors.text,
    fontSize: 12,
    fontWeight: "900",
  },
  ratingButtonTextSelected: {
    color: "#FFFFFF",
  },
  secondaryButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 48,
    paddingHorizontal: 14,
  },
  secondaryButtonText: {
    color: colors.accent,
    fontSize: 16,
    fontWeight: "900",
  },
  smallButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 14,
  },
  smallButtonText: {
    color: "#FFFFFF",
    fontSize: 14,
    fontWeight: "900",
  },
  step: {
    gap: 14,
  },
  stepIntro: {
    gap: 4,
  },
  stepSubtitle: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 20,
  },
  stepTitle: {
    color: colors.text,
    fontSize: 22,
    fontWeight: "900",
  },
  subsection: {
    gap: 9,
  },
  subsectionTitle: {
    color: colors.text,
    fontSize: 16,
    fontWeight: "900",
  },
  title: {
    color: colors.text,
    fontSize: 18,
    fontWeight: "900",
  },
  titleBlock: {
    gap: 3,
  },
  twoColumns: {
    flexDirection: "row",
    gap: 10,
  },
});
