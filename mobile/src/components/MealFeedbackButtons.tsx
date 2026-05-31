import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";

import type { FeedbackType, GeneratedMeal } from "../types/api";

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
              <ActivityIndicator color="#165D77" size="small" />
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
  );
}

function getMealRecipeId(meal: GeneratedMeal): string {
  return String(meal.recipe_id ?? "").trim();
}

const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginTop: 8,
  },
  button: {
    minHeight: 32,
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
    borderColor: "#B7D6DE",
    borderRadius: 8,
    backgroundColor: "#F4FBFC",
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
    color: "#165D77",
    fontSize: 12,
    fontWeight: "800",
  },
  avoidText: {
    color: "#B42318",
  },
  unavailable: {
    color: "#8A6F42",
    fontSize: 12,
    fontWeight: "700",
    marginTop: 8,
  },
});
