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
  const [householdId, setHouseholdId] = useState(defaultHouseholdId);
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
    const cleanHouseholdId = householdId.trim() || defaultHouseholdId;

    if (!cleanName) {
      setValidationError("Display name is required.");
      return;
    }
    if (!isPositiveNumber(parsedAge)) {
      setValidationError("Age must be greater than 0.");
      return;
    }
    if (!isPositiveNumber(parsedWeight)) {
      setValidationError("Weight must be greater than 0.");
      return;
    }
    if (!isPositiveNumber(parsedHeight)) {
      setValidationError("Height must be greater than 0.");
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
      goal_speed: goalSpeed,
      training: {
        sessions_per_week: Number.isFinite(parsedTrainingSessions)
          ? Math.max(0, Math.round(parsedTrainingSessions))
          : 0,
        type: trainingType,
      },
      meal_config: {
        meals_per_day: Number.isFinite(parsedMealsPerDay)
          ? Math.max(1, Math.round(parsedMealsPerDay))
          : 3,
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
        label="Household ID"
        onChangeText={setHouseholdId}
        value={householdId}
      />
      <TextField
        label="Display name"
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

      <OptionGroup label="Sex" options={SEX_OPTIONS} selected={sex} onSelect={setSex} />
      <OptionGroup
        label="Activity"
        options={ACTIVITY_OPTIONS}
        selected={activityLevel}
        onSelect={setActivityLevel}
      />
      <OptionGroup label="Goal" options={GOAL_OPTIONS} selected={goal} onSelect={setGoal} />
      <OptionGroup
        label="Goal speed"
        options={GOAL_SPEED_OPTIONS}
        selected={goalSpeed}
        onSelect={setGoalSpeed}
      />
      <OptionGroup
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

function OptionGroup({
  label,
  onSelect,
  options,
  selected,
}: {
  label: string;
  onSelect: (value: string) => void;
  options: string[];
  selected: string;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.label}>{label}</Text>
      <View style={styles.optionRow}>
        {options.map((option) => (
          <Pressable
            accessibilityRole="button"
            key={option}
            onPress={() => onSelect(option)}
            style={({ pressed }) => [
              styles.optionButton,
              option === selected ? styles.optionButtonSelected : null,
              pressed ? styles.pressed : null,
            ]}
          >
            <Text
              style={[
                styles.optionText,
                option === selected ? styles.optionTextSelected : null,
              ]}
            >
              {option}
            </Text>
          </Pressable>
        ))}
      </View>
    </View>
  );
}

function isPositiveNumber(value: number): boolean {
  return Number.isFinite(value) && value > 0;
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    gap: 12,
    padding: 14,
  },
  disabled: {
    opacity: 0.55,
  },
  errorText: {
    color: "#B42318",
    fontSize: 14,
    fontWeight: "700",
  },
  field: {
    flex: 1,
    gap: 6,
  },
  input: {
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    color: "#111827",
    fontSize: 15,
    minHeight: 44,
    paddingHorizontal: 12,
  },
  label: {
    color: "#4B5563",
    fontSize: 13,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  optionButton: {
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  optionButtonSelected: {
    backgroundColor: "#165D77",
  },
  optionRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  optionText: {
    color: "#165D77",
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
    backgroundColor: "#165D77",
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
    color: "#111827",
    fontSize: 16,
    fontWeight: "800",
  },
  toggle: {
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  toggleSelected: {
    backgroundColor: "#165D77",
  },
  toggleText: {
    color: "#165D77",
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
