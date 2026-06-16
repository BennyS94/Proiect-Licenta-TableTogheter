import { useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Animated,
  LayoutAnimation,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";

import type { FeedbackType, GeneratedMeal } from "../types/api";
import { colors } from "../theme/colors";
import { ChevronDownIcon } from "./icons/ChevronDownIcon";

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
  const chevronProgress = useRef(new Animated.Value(0)).current;
  const isBusy = disabled || Boolean(pendingFeedbackType);
  const controlLabel = selectedFeedbackType
    ? `Rate meal: ${labelForFeedbackType(selectedFeedbackType)}`
    : "Rate meal";

  useEffect(() => {
    Animated.timing(chevronProgress, {
      duration: 220,
      toValue: isExpanded ? 1 : 0,
      useNativeDriver: true,
    }).start();
  }, [chevronProgress, isExpanded]);

  const chevronRotate = chevronProgress.interpolate({
    inputRange: [0, 1],
    outputRange: ["0deg", "180deg"],
  });

  if (!recipeId) {
    return <Text style={styles.unavailable}>Rate meal unavailable for this meal.</Text>;
  }

  async function selectFeedback(feedbackType: FeedbackType) {
    if (isBusy) {
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
    if (isBusy) {
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
            styles.rateControl,
            styles.rateControlExpanded,
            isBusy ? styles.buttonDisabled : null,
          ]}
        >
          <Pressable
            accessibilityRole="button"
            disabled={isBusy}
            onPress={toggleFeedback}
            style={({ pressed }) => [
              styles.rateHeader,
              pressed && !isBusy ? styles.buttonPressed : null,
            ]}
          >
            <Text numberOfLines={1} style={styles.toggleButtonText}>
              {controlLabel}
            </Text>
            <Animated.View style={{ transform: [{ rotate: chevronRotate }] }}>
              <ChevronDownIcon />
            </Animated.View>
          </Pressable>
          <View style={styles.optionRow}>
            {FEEDBACK_ACTIONS.map((action) => {
              const isPending = pendingFeedbackType === action.type;
              const isSelected = selectedFeedbackType === action.type;
              const selectedStyle = FEEDBACK_SELECTED_STYLES[action.type];
              return (
                <Pressable
                  accessibilityRole="button"
                  disabled={isBusy}
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
                    pressed && !isBusy ? styles.buttonPressed : null,
                    isBusy ? styles.buttonDisabled : null,
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
          disabled={isBusy}
          onPress={toggleFeedback}
          style={({ pressed }) => [
            styles.rateControl,
            styles.rateControlCollapsed,
            pressed && !isBusy ? styles.buttonPressed : null,
            isBusy ? styles.buttonDisabled : null,
          ]}
        >
          <Text numberOfLines={1} style={styles.toggleButtonText}>
            {controlLabel}
          </Text>
          <Animated.View style={{ transform: [{ rotate: chevronRotate }] }}>
            <ChevronDownIcon />
          </Animated.View>
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
  rateControl: {
    alignItems: "center",
    backgroundColor: "#FAFCF7",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    paddingHorizontal: 12,
    width: "100%",
  },
  rateControlCollapsed: {
    flexDirection: "row",
    justifyContent: "space-between",
    minHeight: 38,
  },
  rateControlExpanded: {
    alignItems: "stretch",
    gap: 8,
    minHeight: 80,
    paddingVertical: 7,
  },
  rateHeader: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
    justifyContent: "space-between",
    minHeight: 25,
  },
  optionRow: {
    flexDirection: "row",
    gap: 7,
  },
  toggleButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "900",
    flex: 1,
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
