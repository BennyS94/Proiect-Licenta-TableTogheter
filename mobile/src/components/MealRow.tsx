import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import type { FeedbackType, GeneratedMeal, MealReplacementResponse } from "../types/api";
import { MealFeedbackButtons } from "./MealFeedbackButtons";
import { RecipeAlternativesPanel } from "./RecipeAlternativesPanel";

type MealRowProps = {
  meal: GeneratedMeal;
  dayIndex?: number;
  datasetProfile?: string;
  householdId?: string;
  memberProfileId?: string;
  memberProfile?: Record<string, unknown>;
  planId?: string;
  feedbackDisabled?: boolean;
  pendingFeedbackType?: FeedbackType | null;
  onSubmitFeedback?: (meal: GeneratedMeal, feedbackType: FeedbackType) => void;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

export function MealRow({
  meal,
  dayIndex = 1,
  datasetProfile,
  householdId,
  memberProfileId,
  memberProfile,
  planId,
  feedbackDisabled,
  pendingFeedbackType,
  onSubmitFeedback,
  onReplacementApplied,
}: MealRowProps) {
  const [showAlternatives, setShowAlternatives] = useState(false);
  const recipeId = stringValue(meal.recipe_id);
  const slot = stringValue(meal.slot);

  return (
    <View style={styles.container}>
      <View style={styles.row}>
        <View style={styles.main}>
          <Text style={styles.slot}>{String(meal.slot ?? "meal")}</Text>
          <Text style={styles.name}>{String(meal.display_name ?? meal.recipe_id ?? "Recipe")}</Text>
        </View>
        <View style={styles.macros}>
          <Text style={styles.macro}>{formatNumber(meal.kcal)} kcal</Text>
          <Text style={styles.macro}>{formatNumber(meal.protein_g)}g protein</Text>
        </View>
      </View>
      {recipeId ? (
        <Pressable
          accessibilityRole="button"
          onPress={() => setShowAlternatives((current) => !current)}
          style={({ pressed }) => [
            styles.alternativesButton,
            pressed ? styles.alternativesButtonPressed : null,
          ]}
        >
          <Text style={styles.alternativesButtonText}>
            {showAlternatives ? "Hide alternatives" : "Alternatives"}
          </Text>
        </Pressable>
      ) : null}
      {onSubmitFeedback ? (
        <MealFeedbackButtons
          disabled={feedbackDisabled}
          meal={meal}
          onSubmit={onSubmitFeedback}
          pendingFeedbackType={pendingFeedbackType}
        />
      ) : null}
      {recipeId ? (
        <RecipeAlternativesPanel
          dayIndex={dayIndex}
          datasetProfile={datasetProfile}
          generationType="individual"
          householdId={householdId}
          isVisible={showAlternatives}
          memberProfile={memberProfile}
          memberProfileId={memberProfileId}
          onReplacementApplied={onReplacementApplied}
          planId={planId}
          replaceScope="individual_meal"
          slot={slot ?? undefined}
          sourceRecipeId={recipeId}
        />
      ) : null}
    </View>
  );
}

function formatNumber(value: unknown): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return String(Math.round(value));
}

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

const styles = StyleSheet.create({
  alternativesButton: {
    alignSelf: "flex-start",
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  alternativesButtonPressed: {
    opacity: 0.82,
  },
  alternativesButtonText: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
  },
  container: {
    borderTopWidth: 1,
    borderTopColor: "#E5E0D5",
    gap: 8,
    paddingTop: 10,
  },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  main: {
    flex: 1,
    gap: 2,
  },
  slot: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
    textTransform: "capitalize",
  },
  name: {
    color: "#1F2933",
    fontSize: 15,
    fontWeight: "700",
  },
  macros: {
    alignItems: "flex-end",
    minWidth: 92,
  },
  macro: {
    color: "#4B5563",
    fontSize: 13,
    fontWeight: "700",
  },
});
