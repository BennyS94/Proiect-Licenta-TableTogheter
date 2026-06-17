import { useEffect, useState } from "react";
import {
  ActivityIndicator,
  Modal,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import { ChevronDownIcon } from "./icons/ChevronDownIcon";
import { colors } from "../theme/colors";
import type {
  HealthAndDietPreferences,
  MemberProfileCreateRequest,
  MemberProfileResponse,
} from "../types/api";

type AddMemberWizardProps = {
  deleteDisabled?: boolean;
  defaultHouseholdId: string;
  disabled?: boolean;
  forceOpen?: boolean;
  initialProfile?: MemberProfileResponse | null;
  mode?: "add" | "edit";
  onCancel?: () => void;
  onDelete?: () => void;
  onSubmit: (request: MemberProfileCreateRequest) => Promise<void> | void;
};

type WizardStep = 1 | 2 | 3;
type PreferenceRating = "like" | "dislike" | "avoid";
type InfoSheetState = {
  body: string;
  title: string;
} | null;
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
  { label: "Slow pace", value: "slow" },
  { label: "Normal pace", value: "normal" },
  { label: "Aggressive pace", value: "fast" },
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

const DIETARY_PATTERN_OPTIONS: Array<{ label: string; value: DietaryPatternKey }> = [
  { label: "Keto", value: "keto" },
  { label: "Paleo", value: "paleo" },
  { label: "Mediterranean", value: "mediterranean" },
];

const DIETARY_PATTERN_SELECTOR_OPTIONS = [
  { label: "None", value: "none" },
  ...DIETARY_PATTERN_OPTIONS,
];

const HEALTH_MODE_OPTIONS: Array<{ label: string; value: HealthModeKey }> = [
  { label: "Diabetes-aware", value: "diabetes_aware" },
  { label: "Blood-pressure friendly", value: "hypertension_friendly" },
  { label: "Heart-friendly", value: "heart_friendly" },
];

const HEALTH_MODE_SELECTOR_OPTIONS = [
  { label: "None", value: "none" },
  ...HEALTH_MODE_OPTIONS,
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
  deleteDisabled,
  defaultHouseholdId,
  disabled,
  forceOpen,
  initialProfile,
  mode = "add",
  onCancel,
  onDelete,
  onSubmit,
}: AddMemberWizardProps) {
  const [isInternalOpen, setIsInternalOpen] = useState(false);
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
  const [trainingType, setTrainingType] = useState("none");
  const [trainingSessions, setTrainingSessions] = useState("0");
  const [mealsPerDay, setMealsPerDay] = useState("3");
  const [includeSnacks, setIncludeSnacks] = useState(true);
  const [validationError, setValidationError] = useState("");
  const [infoSheet, setInfoSheet] = useState<InfoSheetState>(null);
  const isOpen = Boolean(forceOpen || isInternalOpen);
  const isEditMode = mode === "edit";

  useEffect(() => {
    if (!forceOpen) {
      return;
    }
    if (isEditMode && initialProfile) {
      hydrateDraft(initialProfile);
      return;
    }
    resetDraft();
  }, [forceOpen, initialProfile?.member_profile_id, isEditMode]);

  function resetDraft() {
    setStep(1);
    setDisplayName("");
    setSex("male");
    setAge("");
    setHeightCm("");
    setWeightKg("");
    setDietary(DEFAULT_DIETARY);
    setDietaryPatterns(DEFAULT_DIETARY_PATTERNS);
    setHealthModes(DEFAULT_HEALTH_MODES);
    setRatings({});
    setCustomAvoidInput("");
    setAvoidIngredients([]);
    setGoal("maintain");
    setGoalSpeed("normal");
    setActivityLevel("moderately_active");
    setTrainingType("none");
    setTrainingSessions("0");
    setMealsPerDay("3");
    setIncludeSnacks(true);
    setValidationError("");
  }

  function hydrateDraft(profile: MemberProfileResponse) {
    const training = asRecord(profile.training);
    const mealConfig = asRecord(profile.meal_config);
    const dietaryPreferences = asRecord(profile.dietary_preferences);
    const foodPreferences = profile.food_preferences;
    const healthAndDietPreferences = profile.health_and_diet_preferences;

    setStep(1);
    setDisplayName(profile.display_name || "");
    setSex(profile.sex || "male");
    setAge(String(profile.age || 30));
    setHeightCm(String(profile.height_cm || 175));
    setWeightKg(String(profile.weight_kg || 75));
    setDietary({
      vegetarian: Boolean(dietaryPreferences.vegetarian),
      vegan: Boolean(dietaryPreferences.vegan),
      gluten_free: Boolean(dietaryPreferences.gluten_free),
    });
    setDietaryPatterns({
      keto: Boolean(healthAndDietPreferences?.dietary_patterns?.keto),
      paleo: Boolean(healthAndDietPreferences?.dietary_patterns?.paleo),
      mediterranean: Boolean(
        healthAndDietPreferences?.dietary_patterns?.mediterranean,
      ),
    });
    setHealthModes({
      diabetes_aware: Boolean(
        healthAndDietPreferences?.health_modes?.diabetes_aware,
      ),
      hypertension_friendly: Boolean(
        healthAndDietPreferences?.health_modes?.hypertension_friendly,
      ),
      heart_friendly: Boolean(healthAndDietPreferences?.health_modes?.heart_friendly),
    });
    setRatings(cleanRatingMap(foodPreferences?.ratings ?? {}));
    setCustomAvoidInput("");
    setAvoidIngredients(
      Array.isArray(foodPreferences?.avoid_ingredients)
        ? foodPreferences.avoid_ingredients
        : [],
    );
    setGoal(profile.goal || "maintain");
    setGoalSpeed(profile.goal_speed || "normal");
    setActivityLevel(profile.activity_level || "moderately_active");
    setTrainingType(String(training.type || "none"));
    setTrainingSessions(String(training.sessions_per_week ?? 0));
    setMealsPerDay(String(mealConfig.meals_per_day ?? 3));
    setIncludeSnacks(Boolean(mealConfig.include_snacks ?? true));
    setValidationError("");
  }

  function closeWizard() {
    resetDraft();
    setInfoSheet(null);
    setIsInternalOpen(false);
    onCancel?.();
  }

  function validateCurrentStep(): boolean {
    const cleanName = displayName.trim();
    if (step === 1) {
      if (!cleanName) {
        setValidationError("Please enter a name for this member.");
        return false;
      }
      if (/\d/.test(cleanName)) {
        setValidationError("Please use letters only for the member name.");
        return false;
      }
      if (!age.trim()) {
        setValidationError("Please enter this member's age.");
        return false;
      }
      if (!heightCm.trim()) {
        setValidationError("Please enter this member's height.");
        return false;
      }
      if (!weightKg.trim()) {
        setValidationError("Please enter this member's weight.");
        return false;
      }
      if (!isNumberInRange(Number(age), 4, 120)) {
        setValidationError("Please enter an age between 4 and 120.");
        return false;
      }
      if (!isNumberInRange(Number(heightCm), 80, 230)) {
        setValidationError("Please enter a height between 80 and 230 cm.");
        return false;
      }
      if (!isNumberInRange(Number(weightKg), 15, 300)) {
        setValidationError("Please enter a weight between 15 and 300 kg.");
        return false;
      }
    }
    if (step === 3) {
      if (!isNumberInRange(Number(trainingSessions), 0, 7)) {
        setValidationError("Training sessions can be between 0 and 7 per week.");
        return false;
      }
      if (!isNumberInRange(Number(mealsPerDay), 1, 5)) {
        setValidationError("Meals per day can be between 1 and 5.");
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
      setIsInternalOpen(false);
      onCancel?.();
    } catch (error) {
      setValidationError(formatMemberSaveError(error));
    }
  }

  function buildProfileRequest(): MemberProfileCreateRequest {
    const cleanRatings = cleanRatingMap(ratings);
    const dietaryPreferences = buildDietaryPreferences(dietary, cleanRatings);
    const backendGoal = goal === "balanced" ? "maintain" : goal;
    return {
      household_id: defaultHouseholdId.trim(),
      member_profile_id: isEditMode ? initialProfile?.member_profile_id : undefined,
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
        cooking_time_preference: "balanced",
      },
      health_and_diet_preferences: {
        dietary_patterns: dietaryPatterns,
        health_modes: healthModes,
      },
      bf_profile: "normal",
    };
  }

  function selectNoDietaryRestrictions() {
    setDietary(DEFAULT_DIETARY);
    setRatings((currentRatings) => removeAvoidRatings(currentRatings, VEGAN_AVOID_KEYS));
  }

  function toggleDietaryRestriction(key: keyof DietaryDraft) {
    setDietary((current) => {
      const next = {
        ...current,
        [key]: !current[key],
      };
      if (key === "vegan" && next.vegan) {
        setRatings((currentRatings) => forceAvoidRatings(currentRatings, VEGAN_AVOID_KEYS));
      } else if (key === "vegetarian" && next.vegetarian) {
        setRatings((currentRatings) => forceAvoidRatings(currentRatings, VEGETARIAN_AVOID_KEYS));
      }
      return next;
    });
  }

  function selectDietaryRestriction(value: string) {
    if (value === "none") {
      selectNoDietaryRestrictions();
      return;
    }
    toggleDietaryRestriction(value as keyof DietaryDraft);
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

  function selectDietaryPattern(value: string) {
    setDietaryPatterns({
      keto: value === "keto",
      paleo: value === "paleo",
      mediterranean: value === "mediterranean",
    });
  }

  function selectHealthMode(value: string) {
    setHealthModes({
      diabetes_aware: value === "diabetes_aware",
      hypertension_friendly: value === "hypertension_friendly",
      heart_friendly: value === "heart_friendly",
    });
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
          onPress={() => setIsInternalOpen(true)}
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
      {step === 1 ? (
        <GeneralInfoStep
          age={age}
          displayName={displayName}
          heightCm={heightCm}
          sex={sex}
          weightKg={weightKg}
          onCancel={closeWizard}
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
          onCancel={closeWizard}
          ratings={ratings}
          onAddAvoidIngredient={addAvoidIngredient}
          onCustomAvoidInputChange={setCustomAvoidInput}
          onRemoveAvoidIngredient={removeAvoidIngredient}
          onSelectDietaryRestriction={selectDietaryRestriction}
          onSelectDietaryPattern={selectDietaryPattern}
          onSelectHealthMode={selectHealthMode}
          onShowInfo={setInfoSheet}
          onToggleRating={toggleRating}
        />
      ) : null}

      {step === 3 ? (
        <ActivityGoalStep
          activityLevel={activityLevel}
          goal={goal}
          goalSpeed={goalSpeed}
          includeSnacks={includeSnacks}
          mealsPerDay={mealsPerDay}
          onCancel={closeWizard}
          trainingSessions={trainingSessions}
          trainingType={trainingType}
          onActivityLevelChange={setActivityLevel}
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
              <Text style={styles.primaryButtonText}>
                {isEditMode ? "Save Changes" : "Save Member"}
              </Text>
            )}
          </Pressable>
        )}
      </View>

      {isEditMode && onDelete ? (
        <View style={styles.removeSection}>
          <Pressable
            accessibilityRole="button"
            disabled={deleteDisabled}
            onPress={onDelete}
            style={({ pressed }) => [
              styles.removeButton,
              pressed && !deleteDisabled ? styles.pressed : null,
              deleteDisabled ? styles.disabled : null,
            ]}
          >
            <Text style={styles.removeButtonText}>
              {deleteDisabled ? "Removing member" : "Remove member"}
            </Text>
          </Pressable>
        </View>
      ) : null}
      <InfoSheet info={infoSheet} onClose={() => setInfoSheet(null)} />
    </View>
  );
}

function GeneralInfoStep({
  age,
  displayName,
  heightCm,
  onCancel,
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
  onCancel: () => void;
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
        onCancel={onCancel}
        stepLabel={formatStepLabel(1)}
        subtitle="Tell us about this household member."
        title="General Info"
      />
      <TextField
        label="Name"
        onChangeText={onDisplayNameChange}
        placeholder="Enter name"
        value={displayName}
      />
      <ArrowSelector
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
          placeholder="Age"
          value={age}
        />
        <TextField
          keyboardType="numeric"
          label="Weight"
          onChangeText={onWeightChange}
          placeholder="Weight"
          suffix="kg"
          value={weightKg}
        />
      </View>
      <TextField
        keyboardType="numeric"
        label="Height"
        onChangeText={onHeightChange}
        placeholder="Height"
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
  onCancel,
  ratings,
  onAddAvoidIngredient,
  onCustomAvoidInputChange,
  onRemoveAvoidIngredient,
  onSelectDietaryRestriction,
  onSelectDietaryPattern,
  onSelectHealthMode,
  onShowInfo,
  onToggleRating,
}: {
  avoidIngredients: string[];
  customAvoidInput: string;
  dietary: DietaryDraft;
  dietaryPatterns: DietaryPatternDraft;
  healthModes: HealthModeDraft;
  onCancel: () => void;
  ratings: Record<string, PreferenceRating>;
  onAddAvoidIngredient: () => void;
  onCustomAvoidInputChange: (value: string) => void;
  onRemoveAvoidIngredient: (value: string) => void;
  onSelectDietaryRestriction: (value: string) => void;
  onSelectDietaryPattern: (value: string) => void;
  onSelectHealthMode: (value: string) => void;
  onShowInfo: (info: InfoSheetState) => void;
  onToggleRating: (foodKey: string, rating: PreferenceRating) => void;
}) {
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({});
  const dietaryRestrictionValue = selectedDietaryRestrictionValue(dietary);
  const dietaryPatternValue = selectedDietaryPatternValue(dietaryPatterns);
  const healthModeValue = selectedHealthModeValue(healthModes);

  return (
    <View style={styles.step}>
      <StepIntro
        onCancel={onCancel}
        stepLabel={formatStepLabel(2)}
        subtitle="Choose what this member likes, dislikes or wants to avoid. You can change this later."
        title="Food Preferences"
      />
      <View style={styles.subsection}>
        <Text style={styles.fieldLabel}>Dietary restrictions</Text>
        <View style={styles.dietaryChipRow}>
          <TogglePill
            compact
            label="None"
            onPress={() => onSelectDietaryRestriction("none")}
            selected={dietaryRestrictionValue === "none"}
          />
          <TogglePill
            compact
            label="Vegetarian"
            onPress={() => onSelectDietaryRestriction("vegetarian")}
            selected={dietary.vegetarian}
          />
          <TogglePill
            compact
            label="Vegan"
            onPress={() => onSelectDietaryRestriction("vegan")}
            selected={dietary.vegan}
          />
          <TogglePill
            compact
            label="Gluten free"
            onPress={() => onSelectDietaryRestriction("gluten_free")}
            selected={dietary.gluten_free}
          />
        </View>
      </View>

      <View style={styles.subsection}>
        <SubsectionTitleWithInfo
          onPress={() =>
            onShowInfo({
              title: "Dietary pattern",
              body: "Dietary patterns help TableTogether prioritize meals that match a chosen eating style.",
            })
          }
          title="Dietary pattern"
        />
        <ArrowSelector
          hideLabel
          label="Dietary pattern"
          onSelect={onSelectDietaryPattern}
          options={DIETARY_PATTERN_SELECTOR_OPTIONS}
          selected={dietaryPatternValue}
        />
      </View>

      <View style={styles.subsection}>
        <SubsectionTitleWithInfo
          onPress={() =>
            onShowInfo({
              title: "Health-aware",
              body: "Health-aware preferences help TableTogether prioritize meals. They are not medical advice.",
            })
          }
          title="Health-aware"
        />
        <ArrowSelector
          hideLabel
          label="Health-aware"
          onSelect={onSelectHealthMode}
          options={HEALTH_MODE_SELECTOR_OPTIONS}
          selected={healthModeValue}
        />
      </View>

      {FOOD_SECTIONS.map((section) => (
        <FoodPreferenceGroup
          expanded={Boolean(expandedSections[section.title])}
          key={section.title}
          ratings={ratings}
          section={section}
          onToggle={() =>
            setExpandedSections((current) => ({
              ...current,
              [section.title]: !current[section.title],
            }))
          }
          onToggleRating={onToggleRating}
        />
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
          <View style={styles.customAvoidList}>
            <Text style={styles.customAvoidListTitle}>Custom avoids</Text>
            {avoidIngredients.map((ingredient) => (
              <Pressable
                accessibilityRole="button"
                key={ingredient}
                onPress={() => onRemoveAvoidIngredient(ingredient)}
                style={({ pressed }) => [
                  styles.customAvoidItem,
                  pressed ? styles.pressed : null,
                ]}
              >
                <Text numberOfLines={1} style={styles.customAvoidItemText}>
                  {ingredient}
                </Text>
                <Text style={styles.customAvoidRemove}>x</Text>
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
  goal,
  goalSpeed,
  includeSnacks,
  mealsPerDay,
  onCancel,
  trainingSessions,
  trainingType,
  onActivityLevelChange,
  onGoalChange,
  onGoalSpeedChange,
  onIncludeSnacksChange,
  onMealsPerDayChange,
  onTrainingSessionsChange,
  onTrainingTypeChange,
}: {
  activityLevel: string;
  goal: string;
  goalSpeed: string;
  includeSnacks: boolean;
  mealsPerDay: string;
  onCancel: () => void;
  trainingSessions: string;
  trainingType: string;
  onActivityLevelChange: (value: string) => void;
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
        onCancel={onCancel}
        stepLabel={formatStepLabel(3)}
        subtitle="Set the nutrition goal and daily routine for this member."
        title="Activity & Goal"
      />
      <ArrowSelector
        label="Goal"
        onSelect={onGoalChange}
        options={GOAL_OPTIONS}
        selected={goal}
      />
      {!goalSpeedDisabled ? (
        <ArrowSelector
          label="Goal speed"
          onSelect={onGoalSpeedChange}
          options={GOAL_SPEED_OPTIONS}
          selected={goalSpeed}
        />
      ) : null}
      <ArrowSelector
        label="Activity level"
        onSelect={onActivityLevelChange}
        options={ACTIVITY_OPTIONS}
        selected={activityLevel}
      />
      <ArrowSelector
        label="Training type"
        onSelect={onTrainingTypeChange}
        options={TRAINING_TYPE_OPTIONS}
        selected={trainingType}
      />
      <View style={styles.twoColumns}>
        <StepperField
          label="Sessions/week"
          maximum={7}
          minimum={0}
          onChange={onTrainingSessionsChange}
          value={trainingSessions}
        />
        <StepperField
          label="Meals/day"
          maximum={5}
          minimum={1}
          onChange={onMealsPerDayChange}
          value={mealsPerDay}
        />
      </View>
      <View style={styles.subsection}>
        <Text style={styles.fieldLabel}>Include snack</Text>
        <BinarySegment
          falseLabel="No"
          onChange={onIncludeSnacksChange}
          trueLabel="Yes"
          value={includeSnacks}
        />
      </View>
    </View>
  );
}

function StepIntro({
  onCancel,
  stepLabel,
  subtitle,
  title,
}: {
  onCancel: () => void;
  stepLabel: string;
  subtitle: string;
  title: string;
}) {
  return (
    <View style={styles.stepIntro}>
      <View style={styles.stepHeaderRow}>
        <Text style={styles.stepLabel}>{stepLabel}</Text>
        <Pressable
          accessibilityRole="button"
          onPress={onCancel}
          style={({ pressed }) => [styles.closeButton, pressed ? styles.pressed : null]}
        >
          <Text style={styles.closeText}>Cancel</Text>
        </Pressable>
      </View>
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

function ArrowSelector({
  hideLabel,
  label,
  onSelect,
  options,
  selected,
}: {
  hideLabel?: boolean;
  label: string;
  onSelect: (value: string) => void;
  options: Array<{ label: string; value: string }>;
  selected: string;
}) {
  const selectedIndex = Math.max(
    0,
    options.findIndex((option) => option.value === selected),
  );
  const selectedOption = options[selectedIndex] ?? options[0];

  function selectOffset(offset: number) {
    if (!options.length) {
      return;
    }
    const nextIndex = (selectedIndex + offset + options.length) % options.length;
    onSelect(options[nextIndex].value);
  }

  return (
    <View style={styles.field}>
      {hideLabel ? null : <Text style={styles.fieldLabel}>{label}</Text>}
      <View style={styles.arrowSelector}>
        <Pressable
          accessibilityRole="button"
          onPress={() => selectOffset(-1)}
          style={({ pressed }) => [
            styles.arrowButton,
            pressed ? styles.pressed : null,
          ]}
        >
          <View style={styles.chevronLeft}>
            <ChevronDownIcon color={colors.accent} size={18} />
          </View>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          onPress={() => selectOffset(1)}
          style={({ pressed }) => [
            styles.arrowSelectorValue,
            pressed ? styles.pressed : null,
          ]}
        >
          <Text numberOfLines={1} style={styles.arrowSelectorText}>
            {selectedOption?.label ?? "None"}
          </Text>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          onPress={() => selectOffset(1)}
          style={({ pressed }) => [
            styles.arrowButton,
            pressed ? styles.pressed : null,
          ]}
        >
          <View style={styles.chevronRight}>
            <ChevronDownIcon color={colors.accent} size={18} />
          </View>
        </Pressable>
      </View>
    </View>
  );
}

function FoodPreferenceGroup({
  expanded,
  onToggle,
  onToggleRating,
  ratings,
  section,
}: {
  expanded: boolean;
  onToggle: () => void;
  onToggleRating: (foodKey: string, rating: PreferenceRating) => void;
  ratings: Record<string, PreferenceRating>;
  section: FoodPreferenceSection;
}) {
  const selectedItems = section.items.filter((item) => ratings[item.key]);
  const avoidedItems = selectedItems.filter((item) => ratings[item.key] === "avoid");
  const countLabel = avoidedItems.length
    ? `${avoidedItems.length} avoided`
    : `${selectedItems.length} selected`;

  return (
    <View style={styles.preferenceGroup}>
      <Pressable
        accessibilityRole="button"
        onPress={onToggle}
        style={({ pressed }) => [
          styles.preferenceGroupHeader,
          pressed ? styles.pressed : null,
        ]}
      >
        <Text style={styles.preferenceGroupTitle}>{section.title}</Text>
        <View style={styles.preferenceGroupMeta}>
          <Text style={styles.preferenceGroupCount}>{countLabel}</Text>
          <View style={expanded ? styles.chevronUp : undefined}>
            <ChevronDownIcon color={colors.accent} size={16} />
          </View>
        </View>
      </Pressable>
      {expanded ? (
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
      ) : null}
    </View>
  );
}

function InfoSheet({
  info,
  onClose,
}: {
  info: InfoSheetState;
  onClose: () => void;
}) {
  return (
    <Modal
      animationType="fade"
      onRequestClose={onClose}
      transparent
      visible={Boolean(info)}
    >
      <Pressable style={styles.modalOverlay} onPress={onClose}>
        <Pressable style={styles.infoSheet} onPress={() => undefined}>
          <Text style={styles.infoSheetTitle}>{info?.title}</Text>
          <Text style={styles.infoSheetBody}>{info?.body}</Text>
          <Pressable
            accessibilityRole="button"
            onPress={onClose}
            style={({ pressed }) => [
              styles.modalPrimaryButton,
              pressed ? styles.pressed : null,
            ]}
          >
            <Text style={styles.primaryButtonText}>Got it</Text>
          </Pressable>
        </Pressable>
      </Pressable>
    </Modal>
  );
}

function StepperField({
  label,
  maximum,
  minimum,
  onChange,
  value,
}: {
  label: string;
  maximum: number;
  minimum: number;
  onChange: (value: string) => void;
  value: string;
}) {
  const numericValue = Number.isFinite(Number(value)) ? Number(value) : minimum;
  const safeValue = Math.min(maximum, Math.max(minimum, Math.round(numericValue)));

  function update(delta: number) {
    const next = Math.min(maximum, Math.max(minimum, safeValue + delta));
    onChange(String(next));
  }

  return (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      <View style={styles.stepper}>
        <Pressable
          accessibilityRole="button"
          disabled={safeValue <= minimum}
          onPress={() => update(-1)}
          style={({ pressed }) => [
            styles.stepperButton,
            safeValue <= minimum ? styles.disabled : null,
            pressed && safeValue > minimum ? styles.pressed : null,
          ]}
        >
          <Text style={styles.stepperButtonText}>-</Text>
        </Pressable>
        <Text style={styles.stepperValue}>{safeValue}</Text>
        <Pressable
          accessibilityRole="button"
          disabled={safeValue >= maximum}
          onPress={() => update(1)}
          style={({ pressed }) => [
            styles.stepperButton,
            safeValue >= maximum ? styles.disabled : null,
            pressed && safeValue < maximum ? styles.pressed : null,
          ]}
        >
          <Text style={styles.stepperButtonText}>+</Text>
        </Pressable>
      </View>
    </View>
  );
}

function BinarySegment({
  falseLabel,
  onChange,
  trueLabel,
  value,
}: {
  falseLabel: string;
  onChange: (value: boolean) => void;
  trueLabel: string;
  value: boolean;
}) {
  return (
    <View style={styles.binarySegment}>
      <Pressable
        accessibilityRole="button"
        onPress={() => onChange(true)}
        style={({ pressed }) => [
          styles.binaryOption,
          value ? styles.binaryOptionSelected : null,
          pressed ? styles.pressed : null,
        ]}
      >
        <Text style={[styles.binaryOptionText, value ? styles.binaryOptionTextSelected : null]}>
          {trueLabel}
        </Text>
      </Pressable>
      <Pressable
        accessibilityRole="button"
        onPress={() => onChange(false)}
        style={({ pressed }) => [
          styles.binaryOption,
          !value ? styles.binaryOptionSelected : null,
          pressed ? styles.pressed : null,
        ]}
      >
        <Text style={[styles.binaryOptionText, !value ? styles.binaryOptionTextSelected : null]}>
          {falseLabel}
        </Text>
      </Pressable>
    </View>
  );
}

function SubsectionTitleWithInfo({
  onPress,
  title,
}: {
  onPress: () => void;
  title: string;
}) {
  return (
    <View style={styles.subsectionTitleRow}>
      <Text style={styles.subsectionTitle}>{title}</Text>
      <Pressable
        accessibilityLabel={`${title} information`}
        accessibilityRole="button"
        onPress={onPress}
        style={({ pressed }) => [
          styles.infoButton,
          pressed ? styles.pressed : null,
        ]}
      >
        <Text style={styles.infoButtonText}>i</Text>
      </Pressable>
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
  compact,
  label,
  onPress,
  selected,
}: {
  compact?: boolean;
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
        compact ? styles.pillCompact : null,
        selected ? styles.pillSelected : null,
        pressed ? styles.pressed : null,
      ]}
    >
      <Text
        style={[
          styles.pillText,
          compact ? styles.pillTextCompact : null,
          selected ? styles.pillTextSelected : null,
        ]}
      >
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

function selectedDietaryRestrictionValue(dietary: DietaryDraft): string {
  if (dietary.vegan) {
    return "vegan";
  }
  if (dietary.vegetarian) {
    return "vegetarian";
  }
  if (dietary.gluten_free) {
    return "gluten_free";
  }
  return "none";
}

function selectedDietaryPatternValue(patterns: DietaryPatternDraft): string {
  if (patterns.keto) {
    return "keto";
  }
  if (patterns.paleo) {
    return "paleo";
  }
  if (patterns.mediterranean) {
    return "mediterranean";
  }
  return "none";
}

function selectedHealthModeValue(modes: HealthModeDraft): string {
  if (modes.diabetes_aware) {
    return "diabetes_aware";
  }
  if (modes.hypertension_friendly) {
    return "hypertension_friendly";
  }
  if (modes.heart_friendly) {
    return "heart_friendly";
  }
  return "none";
}

function formatStepLabel(step: WizardStep): string {
  return "Step {step} of 3".replace("{step}", String(step));
}

function formatMemberSaveError(error: unknown): string {
  const message = error instanceof Error ? error.message : "";
  if (!message) {
    return "Could not save this member. Please try again.";
  }
  if (
    message.includes("HTTP ") ||
    message.includes("Cannot reach backend") ||
    message.includes("timed out")
  ) {
    return "Could not save this member. Check the connection and try again.";
  }
  return message;
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

function removeAvoidRatings(
  ratings: Record<string, PreferenceRating>,
  keys: string[],
): Record<string, PreferenceRating> {
  const next = { ...ratings };
  for (const key of keys) {
    if (next[key] === "avoid") {
      delete next[key];
    }
  }
  return next;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function isNumberInRange(value: number, minimum: number, maximum: number): boolean {
  return Number.isFinite(value) && value >= minimum && value <= maximum;
}

const styles = StyleSheet.create({
  arrowButton: {
    alignItems: "center",
    justifyContent: "center",
    minHeight: 42,
    width: 42,
  },
  arrowSelector: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    minHeight: 46,
  },
  arrowSelectorText: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "center",
  },
  arrowSelectorValue: {
    alignItems: "center",
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
    minWidth: 0,
    paddingHorizontal: 6,
  },
  avoidButton: {
    borderColor: "#C2410C",
  },
  avoidButtonSelected: {
    backgroundColor: "#B42318",
    borderColor: "#B42318",
  },
  chevronLeft: {
    transform: [{ rotate: "90deg" }],
  },
  chevronRight: {
    transform: [{ rotate: "-90deg" }],
  },
  chevronUp: {
    transform: [{ rotate: "180deg" }],
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
    gap: 12,
    padding: 14,
    paddingBottom: 18,
  },
  customAvoidItem: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
    minHeight: 38,
    paddingHorizontal: 10,
  },
  customAvoidItemText: {
    color: colors.text,
    flex: 1,
    fontSize: 14,
    fontWeight: "800",
  },
  customAvoidList: {
    gap: 7,
  },
  customAvoidListTitle: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "900",
    textTransform: "uppercase",
  },
  customAvoidRemove: {
    color: colors.danger,
    fontSize: 16,
    fontWeight: "900",
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
  dietaryChipRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
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
    alignItems: "flex-end",
    flexDirection: "row",
    justifyContent: "flex-end",
  },
  helperText: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
    lineHeight: 18,
  },
  infoButton: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 999,
    borderWidth: 1,
    height: 24,
    justifyContent: "center",
    width: 24,
  },
  infoButtonActive: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  infoButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "900",
  },
  infoButtonTextActive: {
    color: "#FFFFFF",
  },
  infoNote: {
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
    lineHeight: 18,
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  infoSheet: {
    backgroundColor: "#FFFFFF",
    borderColor: colors.border,
    borderRadius: 18,
    borderWidth: 1,
    gap: 12,
    marginHorizontal: 22,
    padding: 18,
  },
  infoSheetBody: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 20,
  },
  infoSheetTitle: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900",
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
  pillCompact: {
    minHeight: 34,
    paddingHorizontal: 9,
    paddingVertical: 7,
  },
  pillText: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
  },
  pillTextCompact: {
    fontSize: 12,
    fontWeight: "900",
  },
  pillTextSelected: {
    color: "#FFFFFF",
  },
  preferenceActions: {
    flexDirection: "row",
    flexShrink: 0,
    gap: 3,
  },
  preferenceGroup: {
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    overflow: "hidden",
  },
  preferenceGroupCount: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
  },
  preferenceGroupHeader: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
    minHeight: 46,
    paddingHorizontal: 12,
  },
  preferenceGroupMeta: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  preferenceGroupTitle: {
    color: colors.text,
    flex: 1,
    fontSize: 15,
    fontWeight: "900",
  },
  preferenceLabel: {
    color: colors.text,
    flex: 1,
    fontSize: 14,
    fontWeight: "900",
    minWidth: 96,
  },
  preferenceRow: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 10,
    minHeight: 48,
    paddingHorizontal: 9,
    paddingVertical: 7,
  },
  preferenceRows: {
    gap: 8,
    padding: 10,
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
    minHeight: 31,
    minWidth: 50,
    paddingHorizontal: 4,
  },
  ratingButtonSelected: {
    borderWidth: 1,
  },
  ratingButtonText: {
    color: colors.text,
    fontSize: 11,
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
  removeButton: {
    alignItems: "center",
    borderColor: colors.danger,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 14,
  },
  removeButtonText: {
    color: colors.danger,
    fontSize: 14,
    fontWeight: "900",
  },
  removeSection: {
    borderTopColor: colors.border,
    borderTopWidth: 1,
    gap: 10,
    paddingTop: 12,
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
  binaryOption: {
    alignItems: "center",
    borderRadius: 7,
    flex: 1,
    justifyContent: "center",
    minHeight: 36,
  },
  binaryOptionSelected: {
    backgroundColor: colors.accent,
  },
  binaryOptionText: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "900",
  },
  binaryOptionTextSelected: {
    color: "#FFFFFF",
  },
  binarySegment: {
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 4,
    padding: 4,
  },
  step: {
    gap: 14,
  },
  stepLabel: {
    color: colors.accent,
    fontSize: 12,
    fontWeight: "900",
    textTransform: "uppercase",
  },
  stepIntro: {
    gap: 5,
  },
  stepHeaderRow: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
  },
  stepSubtitle: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 20,
  },
  stepTitle: {
    color: colors.text,
    fontSize: 21,
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
  subsectionTitleRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
    justifyContent: "space-between",
  },
  modalOverlay: {
    backgroundColor: "rgba(17, 24, 39, 0.32)",
    flex: 1,
    justifyContent: "center",
  },
  modalPrimaryButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    justifyContent: "center",
    minHeight: 46,
    paddingHorizontal: 14,
  },
  stepper: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    minHeight: 46,
    padding: 4,
  },
  stepperButton: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: colors.border,
    borderRadius: 7,
    borderWidth: 1,
    height: 34,
    justifyContent: "center",
    width: 34,
  },
  stepperButtonText: {
    color: colors.accent,
    fontSize: 20,
    fontWeight: "900",
    lineHeight: 22,
  },
  stepperValue: {
    color: colors.text,
    flex: 1,
    fontSize: 16,
    fontWeight: "900",
    textAlign: "center",
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
