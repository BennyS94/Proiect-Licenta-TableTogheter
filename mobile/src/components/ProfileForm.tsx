import { useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import type { MemberProfileCreateRequest } from "../types/api";
import { colors } from "../theme/colors";

type ProfileFormProps = {
  defaultHouseholdId: string;
  disabled?: boolean;
  onSubmit: (request: MemberProfileCreateRequest) => Promise<void> | void;
};

const SEX_OPTIONS = ["male", "female"];
const ACTIVITY_OPTIONS = ["lightly_active", "moderately_active", "very_active"];
const GOAL_OPTIONS = ["maintain", "lose", "gain"];
const GOAL_SPEED_OPTIONS = ["slow", "normal", "fast"];
const TRAINING_TYPE_OPTIONS = ["mixed", "weights", "cardio"];

export function ProfileForm({ defaultHouseholdId, disabled, onSubmit }: ProfileFormProps) {
  const [displayName, setDisplayName] = useState("");
  const [age, setAge] = useState("30");
  const [sex, setSex] = useState("male");
  const [weightKg, setWeightKg] = useState("75");
  const [heightCm, setHeightCm] = useState("175");
  const [activityLevel, setActivityLevel] = useState("moderately_active");
  const [goal, setGoal] = useState("maintain");
  const [goalSpeed, setGoalSpeed] = useState("normal");
  const [trainingSessions, setTrainingSessions] = useState("3");
  const [trainingType, setTrainingType] = useState("mixed");
  const [mealsPerDay, setMealsPerDay] = useState("3");
  const [includeSnacks, setIncludeSnacks] = useState(true);
  const [validationError, setValidationError] = useState("");

  async function handleSubmit() {
    const parsedAge = Number(age);
    const parsedWeight = Number(weightKg);
    const parsedHeight = Number(heightCm);
    const parsedTrainingSessions = Number(trainingSessions);
    const parsedMealsPerDay = Number(mealsPerDay);
    const cleanName = displayName.trim();
    const cleanHouseholdId = defaultHouseholdId.trim();

    if (!cleanName) {
      setValidationError("Display name is required.");
      return;
    }
    if (/\d/.test(cleanName)) {
      setValidationError("Name cannot contain numbers.");
      return;
    }
    if (!isNumberInRange(parsedAge, 4, 120)) {
      setValidationError("Age must be between 4 and 120.");
      return;
    }
    if (!isNumberInRange(parsedWeight, 15, 300)) {
      setValidationError("Weight must be between 15 and 300 kg.");
      return;
    }
    if (!isNumberInRange(parsedHeight, 80, 230)) {
      setValidationError("Height must be between 80 and 230 cm.");
      return;
    }
    if (!isNumberInRange(parsedTrainingSessions, 0, 7)) {
      setValidationError("Sessions per week must be between 0 and 7.");
      return;
    }
    if (!isNumberInRange(parsedMealsPerDay, 1, 5)) {
      setValidationError("Meals per day must be between 1 and 5.");
      return;
    }

    setValidationError("");
    await onSubmit({
      household_id: cleanHouseholdId,
      display_name: cleanName,
      age: Math.round(parsedAge),
      sex,
      weight_kg: parsedWeight,
      height_cm: parsedHeight,
      activity_level: activityLevel,
      goal,
      goal_speed: goal === "maintain" ? "normal" : goalSpeed,
      training: {
        sessions_per_week: Math.round(parsedTrainingSessions),
        type: trainingType,
      },
      meal_config: {
        meals_per_day: Math.round(parsedMealsPerDay),
        include_snacks: includeSnacks,
        day_structure: includeSnacks ? "3_meals_plus_snack" : "3_meals",
      },
      dietary_preferences: {
        no_beef: false,
        no_chicken: false,
        no_fish: false,
        no_dairy: false,
        vegetarian: false,
        vegan: false,
        gluten_free: false,
      },
      bf_profile: "normal",
    });
  }

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Create profile</Text>

      <TextField
        label="Name"
        onChangeText={setDisplayName}
        placeholder="Alex"
        value={displayName}
      />
      <View style={styles.twoColumns}>
        <TextField
          keyboardType="numeric"
          label="Age"
          onChangeText={setAge}
          value={age}
        />
        <TextField
          keyboardType="numeric"
          label="Weight kg"
          onChangeText={setWeightKg}
          value={weightKg}
        />
      </View>
      <TextField
        keyboardType="numeric"
        label="Height cm"
        onChangeText={setHeightCm}
        value={heightCm}
      />

      <StepperSelector label="Sex" options={SEX_OPTIONS} selected={sex} onSelect={setSex} />
      <StepperSelector
        label="Activity"
        options={ACTIVITY_OPTIONS}
        selected={activityLevel}
        onSelect={setActivityLevel}
      />
      <StepperSelector label="Goal" options={GOAL_OPTIONS} selected={goal} onSelect={setGoal} />
      <StepperSelector
        disabled={goal === "maintain"}
        label="Goal speed"
        options={GOAL_SPEED_OPTIONS}
        selected={goalSpeed}
        onSelect={setGoalSpeed}
      />
      <StepperSelector
        label="Training type"
        options={TRAINING_TYPE_OPTIONS}
        selected={trainingType}
        onSelect={setTrainingType}
      />

      <View style={styles.twoColumns}>
        <TextField
          keyboardType="numeric"
          label="Sessions/week"
          onChangeText={setTrainingSessions}
          value={trainingSessions}
        />
        <TextField
          keyboardType="numeric"
          label="Meals/day"
          onChangeText={setMealsPerDay}
          value={mealsPerDay}
        />
      </View>

      <Pressable
        accessibilityRole="button"
        onPress={() => setIncludeSnacks((current) => !current)}
        style={({ pressed }) => [
          styles.toggle,
          includeSnacks ? styles.toggleSelected : null,
          pressed ? styles.pressed : null,
        ]}
      >
        <Text style={[styles.toggleText, includeSnacks ? styles.toggleTextSelected : null]}>
          Include snacks: {includeSnacks ? "yes" : "no"}
        </Text>
      </Pressable>

      {validationError ? <Text style={styles.errorText}>{validationError}</Text> : null}

      <Pressable
        accessibilityRole="button"
        disabled={disabled}
        onPress={handleSubmit}
        style={({ pressed }) => [
          styles.submitButton,
          pressed && !disabled ? styles.pressed : null,
          disabled ? styles.disabled : null,
        ]}
      >
        {disabled ? (
          <ActivityIndicator color="#FFFFFF" />
        ) : (
          <Text style={styles.submitText}>Save profile</Text>
        )}
      </Pressable>
    </View>
  );
}

function TextField({
  keyboardType,
  label,
  onChangeText,
  placeholder,
  value,
}: {
  keyboardType?: "default" | "numeric";
  label: string;
  onChangeText: (value: string) => void;
  placeholder?: string;
  value: string;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.label}>{label}</Text>
      <TextInput
        keyboardType={keyboardType ?? "default"}
        onChangeText={onChangeText}
        placeholder={placeholder}
        style={styles.input}
        value={value}
      />
    </View>
  );
}

function StepperSelector({
  disabled,
  label,
  onSelect,
  options,
  selected,
}: {
  disabled?: boolean;
  label: string;
  onSelect: (value: string) => void;
  options: string[];
  selected: string;
}) {
  const currentIndex = Math.max(0, options.indexOf(selected));
  const previousValue =
    options[currentIndex <= 0 ? options.length - 1 : currentIndex - 1] ?? selected;
  const nextValue =
    options[currentIndex >= options.length - 1 ? 0 : currentIndex + 1] ?? selected;

  return (
    <View style={styles.field}>
      <Text style={styles.label}>{label}</Text>
      <View style={styles.stepper}>
        <Pressable
          accessibilityRole="button"
          disabled={disabled}
          onPress={() => onSelect(previousValue)}
          style={({ pressed }) => [
            styles.stepperButton,
            disabled ? styles.stepperButtonDisabled : null,
            pressed && !disabled ? styles.pressed : null,
          ]}
        >
          <Text style={styles.stepperArrow}>{"<"}</Text>
        </Pressable>
        <Text style={[styles.stepperValue, disabled ? styles.stepperValueDisabled : null]}>
          {disabled ? "Not needed" : formatOption(selected)}
        </Text>
        <Pressable
          accessibilityRole="button"
          disabled={disabled}
          onPress={() => onSelect(nextValue)}
          style={({ pressed }) => [
            styles.stepperButton,
            disabled ? styles.stepperButtonDisabled : null,
            pressed && !disabled ? styles.pressed : null,
          ]}
        >
          <Text style={styles.stepperArrow}>{">"}</Text>
        </Pressable>
      </View>
    </View>
  );
}

function formatOption(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function isNumberInRange(value: number, minimum: number, maximum: number): boolean {
  return Number.isFinite(value) && value >= minimum && value <= maximum;
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 12,
    padding: 14,
  },
  disabled: {
    opacity: 0.55,
  },
  errorText: {
    color: colors.danger,
    fontSize: 14,
    fontWeight: "700",
  },
  field: {
    flex: 1,
    gap: 6,
  },
  input: {
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    color: colors.text,
    fontSize: 15,
    minHeight: 44,
    paddingHorizontal: 12,
    textAlign: "center",
  },
  label: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  stepper: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    justifyContent: "space-between",
    minHeight: 46,
    overflow: "hidden",
  },
  stepperArrow: {
    color: "#FFFFFF",
    fontSize: 18,
    fontWeight: "900",
  },
  stepperButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    height: 46,
    justifyContent: "center",
    width: 48,
  },
  stepperButtonDisabled: {
    backgroundColor: "#D1D5DB",
  },
  stepperValue: {
    color: colors.text,
    flex: 1,
    fontSize: 15,
    fontWeight: "800",
    textAlign: "center",
  },
  stepperValueDisabled: {
    color: colors.mutedSoft,
  },
  optionButton: {
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  optionButtonSelected: {
    backgroundColor: colors.accent,
  },
  optionRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  optionText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "800",
  },
  optionTextSelected: {
    color: "#FFFFFF",
  },
  pressed: {
    opacity: 0.82,
  },
  submitButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    justifyContent: "center",
    minHeight: 48,
    paddingHorizontal: 16,
  },
  submitText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "800",
  },
  title: {
    color: colors.text,
    fontSize: 16,
    fontWeight: "800",
  },
  toggle: {
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  toggleSelected: {
    backgroundColor: colors.accent,
  },
  toggleText: {
    color: colors.accent,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "center",
  },
  toggleTextSelected: {
    color: "#FFFFFF",
  },
  twoColumns: {
    flexDirection: "row",
    gap: 10,
  },
});
