import { useState } from "react";
import {
  ActivityIndicator,
  LayoutAnimation,
  Platform,
  Pressable,
  StyleSheet,
  Text,
  UIManager,
  View,
} from "react-native";

import type { FeedbackType, GeneratedMeal } from "../types/api";
import { colors } from "../theme/colors";

type MealFeedbackButtonsProps = {
  meal: GeneratedMeal;
  disabled?: boolean;
  pendingFeedbackType?: FeedbackType | null;
  onSubmit: (meal: GeneratedMeal, feedbackType: FeedbackType) => Promise<void> | void;
  onUndo?: (meal: GeneratedMeal, feedbackType: FeedbackType) => Promise<void> | void;
};

const FEEDBACK_ACTIONS: Array<{ label: string; type: FeedbackType }> = [
  { label: "Like", type: "liked" },
  { label: "Dislike", type: "disliked" },
  { label: "Too long", type: "too_long" },
];

const FEEDBACK_SELECTED_STYLES: Record<
  FeedbackType,
  {
    backgroundColor: string;
    borderColor: string;
    textColor: string;
  }
> = {
  explicit_avoid: {
    backgroundColor: "#FEE2E2",
    borderColor: "#DC2626",
    textColor: "#991B1B",
  },
  disliked: {
    backgroundColor: "#FEE2E2",
    borderColor: "#DC2626",
    textColor: "#991B1B",
  },
  liked: {
    backgroundColor: "#EEF7E8",
    borderColor: colors.accent,
    textColor: colors.accentDark,
  },
  too_long: {
    backgroundColor: "#FFF8DB",
    borderColor: "#D6A800",
    textColor: "#8A6500",
  },
};

if (Platform.OS === "android") {
  UIManager.setLayoutAnimationEnabledExperimental?.(true);
}

export function MealFeedbackButtons({
  meal,
  disabled,
  pendingFeedbackType,
  onSubmit,
  onUndo,
}: MealFeedbackButtonsProps) {
  const recipeId = getMealRecipeId(meal);
  const [selectedFeedbackType, setSelectedFeedbackType] = useState<FeedbackType | null>(null);
  const [isExpanded, setIsExpanded] = useState(false);

  if (!recipeId) {
    return <Text style={styles.unavailable}>Feedback unavailable for this meal.</Text>;
  }

  async function selectFeedback(feedbackType: FeedbackType) {
    if (disabled || pendingFeedbackType) {
      return;
    }
    if (selectedFeedbackType === feedbackType) {
      setSelectedFeedbackType(null);
      await onUndo?.(meal, feedbackType);
      return;
    }

    setSelectedFeedbackType(feedbackType);
    await onSubmit(meal, feedbackType);
  }

  function toggleFeedback() {
    if (disabled || pendingFeedbackType) {
      return;
    }
    animateFeedbackLayout();
    setIsExpanded((current) => !current);
  }

  return (
    <View style={styles.container}>
      {isExpanded ? (
        <View
          style={[
            styles.feedbackControl,
            styles.feedbackControlExpanded,
            disabled || pendingFeedbackType ? styles.buttonDisabled : null,
          ]}
        >
          <Pressable
            accessibilityRole="button"
            disabled={disabled || Boolean(pendingFeedbackType)}
            onPress={toggleFeedback}
            style={({ pressed }) => [
              styles.feedbackHeader,
              pressed && !disabled ? styles.buttonPressed : null,
            ]}
          >
            <Text style={styles.toggleButtonText}>
              {selectedFeedbackType
                ? `Feedback: ${labelForFeedbackType(selectedFeedbackType)}`
                : "Feedback"}{" "}
              ^
            </Text>
          </Pressable>
          <View style={styles.optionRow}>
            {FEEDBACK_ACTIONS.map((action) => {
              const isPending = pendingFeedbackType === action.type;
              const isSelected = selectedFeedbackType === action.type;
              const selectedStyle = FEEDBACK_SELECTED_STYLES[action.type];
              return (
                <Pressable
                  accessibilityRole="button"
                  disabled={disabled || Boolean(pendingFeedbackType)}
                  key={action.type}
                  onPress={() => {
                    void selectFeedback(action.type);
                  }}
                  style={({ pressed }) => [
                    styles.optionButton,
                    isSelected
                      ? {
                          backgroundColor: selectedStyle.backgroundColor,
                          borderColor: selectedStyle.borderColor,
                        }
                      : null,
                    pressed && !disabled ? styles.buttonPressed : null,
                    disabled || pendingFeedbackType ? styles.buttonDisabled : null,
                  ]}
                >
                  {isPending ? (
                    <ActivityIndicator color={selectedStyle.borderColor} size="small" />
                  ) : (
                    <Text
                      style={[
                        styles.buttonText,
                        isSelected ? { color: selectedStyle.textColor } : null,
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
      ) : (
        <Pressable
          accessibilityRole="button"
          disabled={disabled || Boolean(pendingFeedbackType)}
          onPress={toggleFeedback}
          style={({ pressed }) => [
            styles.feedbackControl,
            styles.feedbackControlCollapsed,
            pressed && !disabled ? styles.buttonPressed : null,
            disabled || pendingFeedbackType ? styles.buttonDisabled : null,
          ]}
        >
          <Text style={styles.toggleButtonText}>
            {selectedFeedbackType
              ? `Feedback: ${labelForFeedbackType(selectedFeedbackType)}`
              : "Feedback"}{" "}
            v
          </Text>
        </Pressable>
      )}
    </View>
  );
}

function animateFeedbackLayout() {
  LayoutAnimation.configureNext({
    create: {
      property: LayoutAnimation.Properties.opacity,
      type: LayoutAnimation.Types.easeInEaseOut,
    },
    delete: {
      property: LayoutAnimation.Properties.opacity,
      type: LayoutAnimation.Types.easeInEaseOut,
    },
    duration: 150,
    update: {
      type: LayoutAnimation.Types.easeInEaseOut,
    },
  });
}

function labelForFeedbackType(feedbackType: FeedbackType): string {
  return FEEDBACK_ACTIONS.find((action) => action.type === feedbackType)?.label ?? "Selected";
}

function getMealRecipeId(meal: GeneratedMeal): string {
  return String(meal.recipe_id ?? "").trim();
}

const styles = StyleSheet.create({
  container: {
    marginTop: 2,
  },
  feedbackControl: {
    alignItems: "center",
    backgroundColor: "#FAFCF7",
    borderColor: "#CFE3BF",
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    paddingHorizontal: 12,
    width: "100%",
  },
  feedbackControlCollapsed: {
    minHeight: 34,
  },
  feedbackControlExpanded: {
    alignItems: "stretch",
    gap: 7,
    minHeight: 74,
    paddingVertical: 7,
  },
  feedbackHeader: {
    alignItems: "center",
    justifyContent: "center",
    minHeight: 25,
  },
  optionRow: {
    flexDirection: "row",
    gap: 8,
  },
  toggleButtonText: {
    color: colors.accentDark,
    fontSize: 13,
    fontWeight: "900",
  },
  optionButton: {
    alignItems: "center",
    backgroundColor: "#FAFCF7",
    borderColor: "#DDEAD3",
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 30,
    paddingHorizontal: 8,
  },
  buttonPressed: {
    opacity: 0.8,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  buttonText: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "800",
  },
  unavailable: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "700",
    marginTop: 8,
  },
});
