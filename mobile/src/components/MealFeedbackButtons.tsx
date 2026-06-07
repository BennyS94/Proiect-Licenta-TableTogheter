import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";

import type { FeedbackType, GeneratedMeal } from "../types/api";
import { colors } from "../theme/colors";

type MealFeedbackButtonsProps = {
  meal: GeneratedMeal;
  disabled?: boolean;
  pendingFeedbackType?: FeedbackType | null;
  onSubmit: (meal: GeneratedMeal, feedbackType: FeedbackType) => void;
};

const FEEDBACK_ACTIONS: Array<{ label: string; type: FeedbackType }> = [
  { label: "Like", type: "liked" },
  { label: "Dislike", type: "disliked" },
  { label: "Too long", type: "too_long" },
  { label: "Avoid", type: "explicit_avoid" },
];

export function MealFeedbackButtons({
  meal,
  disabled,
  pendingFeedbackType,
  onSubmit,
}: MealFeedbackButtonsProps) {
  const recipeId = getMealRecipeId(meal);

  if (!recipeId) {
    return <Text style={styles.unavailable}>Feedback unavailable for this meal.</Text>;
  }

  return (
    <View style={styles.container}>
      <View style={styles.labelRow}>
        <Text style={styles.labelText}>Liked</Text>
        <Text style={styles.labelText}>Avoided</Text>
        <Text style={styles.labelText}>Too long</Text>
      </View>
      <View style={styles.row}>
        {FEEDBACK_ACTIONS.map((action) => {
          const isPending = pendingFeedbackType === action.type;
          return (
            <Pressable
              accessibilityRole="button"
              disabled={disabled || Boolean(pendingFeedbackType)}
              key={action.type}
              onPress={() => onSubmit(meal, action.type)}
              style={({ pressed }) => [
                styles.button,
                action.type === "explicit_avoid" ? styles.avoidButton : null,
                pressed && !disabled ? styles.buttonPressed : null,
                disabled || pendingFeedbackType ? styles.buttonDisabled : null,
              ]}
            >
              {isPending ? (
                <ActivityIndicator color={colors.accent} size="small" />
              ) : (
                <Text
                  style={[
                    styles.buttonText,
                    action.type === "explicit_avoid" ? styles.avoidText : null,
                  ]}
                >
                  {action.label}
                </Text>
              )}
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

function getMealRecipeId(meal: GeneratedMeal): string {
  return String(meal.recipe_id ?? "").trim();
}

const styles = StyleSheet.create({
  container: {
    gap: 6,
    marginTop: 8,
  },
  labelRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  labelText: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
  },
  row: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  button: {
    minHeight: 32,
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 8,
    backgroundColor: colors.background,
    paddingHorizontal: 10,
  },
  avoidButton: {
    borderColor: "#F1B3A7",
    backgroundColor: "#FFF5F2",
  },
  buttonPressed: {
    opacity: 0.8,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  buttonText: {
    color: colors.accent,
    fontSize: 12,
    fontWeight: "800",
  },
  avoidText: {
    color: colors.danger,
  },
  unavailable: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "700",
    marginTop: 8,
  },
});
