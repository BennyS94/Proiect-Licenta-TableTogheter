import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";

import {
  applyMealReplacement,
  getRecipeAlternatives,
  previewMealReplacement,
} from "../services/apiClient";
import type {
  MealReplacementRequest,
  MealReplacementResponse,
  MealReplacementScope,
  RecipeAlternativeItem,
  RecipeAlternativesResponse,
} from "../types/api";
import { colors } from "../theme/colors";

type RecipeAlternativesPanelProps = {
  sourceRecipeId: string;
  slot?: string;
  dayIndex?: number;
  datasetProfile?: string;
  generationType?: "individual" | "household";
  householdId?: string;
  memberId?: string;
  memberProfileId?: string;
  memberProfile?: Record<string, unknown>;
  planId?: string;
  replaceScope?: MealReplacementScope;
  isVisible?: boolean;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

const DEFAULT_DATASET_PROFILE = "v1_2_demo_final";

export function RecipeAlternativesPanel({
  sourceRecipeId,
  slot,
  dayIndex = 1,
  datasetProfile = DEFAULT_DATASET_PROFILE,
  generationType = "individual",
  householdId,
  memberId,
  memberProfileId,
  memberProfile,
  planId,
  replaceScope,
  isVisible = true,
  onReplacementApplied,
}: RecipeAlternativesPanelProps) {
  const [response, setResponse] = useState<RecipeAlternativesResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [hasLoaded, setHasLoaded] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [previewResponse, setPreviewResponse] = useState<MealReplacementResponse | null>(null);
  const [previewedRecipeId, setPreviewedRecipeId] = useState("");
  const [previewingRecipeId, setPreviewingRecipeId] = useState("");
  const [applyingRecipeId, setApplyingRecipeId] = useState("");
  const [selectedAlternativeIndex, setSelectedAlternativeIndex] = useState(0);
  const [successMessage, setSuccessMessage] = useState("");

  useEffect(() => {
    setResponse(null);
    setErrorMessage("");
    setHasLoaded(false);
    setPreviewResponse(null);
    setPreviewedRecipeId("");
    setPreviewingRecipeId("");
    setApplyingRecipeId("");
    setSelectedAlternativeIndex(0);
    setSuccessMessage("");
  }, [
    datasetProfile,
    dayIndex,
    generationType,
    householdId,
    memberId,
    memberProfile,
    memberProfileId,
    planId,
    slot,
    sourceRecipeId,
  ]);

  useEffect(() => {
    let isCurrent = true;

    async function loadAlternatives() {
      if (!isVisible || hasLoaded || !sourceRecipeId) {
        return;
      }

      setIsLoading(true);
      setErrorMessage("");
      try {
        const payload = await getRecipeAlternatives({
          recipe_id: sourceRecipeId,
          slot,
          top_k: 5,
          candidate_pool_k: 20,
          dataset_profile: datasetProfile,
          household_id: householdId,
          member_profile_id: memberProfileId,
          member_profile: memberProfile,
          feedback_enabled: true,
          approval_mode: "include_review",
        });
        if (isCurrent) {
          setResponse(payload);
          setSelectedAlternativeIndex(0);
          setHasLoaded(true);
        }
      } catch (error) {
        if (isCurrent) {
          setResponse(null);
          setErrorMessage(
            error instanceof Error ? error.message : "Recipe alternatives failed",
          );
        }
      } finally {
        if (isCurrent) {
          setIsLoading(false);
        }
      }
    }

    void loadAlternatives();

    return () => {
      isCurrent = false;
    };
  }, [
    datasetProfile,
    hasLoaded,
    householdId,
    isVisible,
    memberProfile,
    memberProfileId,
    slot,
    sourceRecipeId,
  ]);

  const alternatives = useMemo(
    () =>
      (response?.alternatives ?? []).filter((alternative) => {
        const status = stringValue(alternative.approval_status);
        return status === "approved" || status === "review";
      }),
    [response],
  );
  const currentAlternative = alternatives[selectedAlternativeIndex] ?? alternatives[0] ?? null;
  const canRequestReplacement = Boolean(planId && slot && sourceRecipeId);

  useEffect(() => {
    if (alternatives.length && selectedAlternativeIndex >= alternatives.length) {
      setSelectedAlternativeIndex(0);
    }
  }, [alternatives.length, selectedAlternativeIndex]);

  async function previewReplacement(alternative: RecipeAlternativeItem) {
    if (!planId || !slot) {
      setErrorMessage("Generate and save a plan before previewing replacement.");
      return;
    }

    const request = buildReplacementRequest(alternative.recipe_id);
    setPreviewingRecipeId(alternative.recipe_id);
    setPreviewResponse(null);
    setPreviewedRecipeId("");
    setSuccessMessage("");
    setErrorMessage("");

    try {
      const payload = await previewMealReplacement(planId, request);
      setPreviewResponse(payload);
      setPreviewedRecipeId(alternative.recipe_id);
    } catch (error) {
      setPreviewResponse(null);
      setErrorMessage(error instanceof Error ? error.message : "Replacement preview failed");
    } finally {
      setPreviewingRecipeId("");
    }
  }

  async function applyReplacement() {
    if (!planId || !previewResponse?.replacement) {
      return;
    }
    const alternativeMeal = previewResponse.replacement.alternative_meal;
    const alternativeRecipeId = stringValue(alternativeMeal?.recipe_id);
    if (!alternativeRecipeId) {
      setErrorMessage("Replacement preview is missing the alternative recipe.");
      return;
    }

    const request = buildReplacementRequest(alternativeRecipeId);
    setApplyingRecipeId(alternativeRecipeId);
    setErrorMessage("");
    setSuccessMessage("");

    try {
      const payload = await applyMealReplacement(planId, request);
      setPreviewResponse(payload);
      setPreviewedRecipeId(alternativeRecipeId);
      setSuccessMessage("Meal replaced. Plan and grocery list updated.");
      onReplacementApplied?.(payload);
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Meal replacement failed");
    } finally {
      setApplyingRecipeId("");
    }
  }

  function buildReplacementRequest(alternativeRecipeId: string): MealReplacementRequest {
    return {
      day_index: dayIndex,
      slot: slot ?? "",
      current_recipe_id: sourceRecipeId,
      alternative_recipe_id: alternativeRecipeId,
      generation_type: generationType,
      replace_scope: replaceScope,
      member_id: memberId,
      member_profile_id: memberProfileId,
      dataset_profile: datasetProfile,
      feedback_enabled: true,
    };
  }

  function showNextAlternative() {
    if (alternatives.length <= 1) {
      return;
    }
    setSelectedAlternativeIndex((current) => (current + 1) % alternatives.length);
    setPreviewResponse(null);
    setPreviewedRecipeId("");
    setSuccessMessage("");
    setErrorMessage("");
  }

  if (!isVisible) {
    return null;
  }

  return (
    <View style={styles.container}>
      {!canRequestReplacement ? (
        <Text style={styles.mutedText}>Generate a plan before replacing meals.</Text>
      ) : null}

      {isLoading ? (
        <View style={styles.loadingRow}>
          <ActivityIndicator color={colors.accent} />
          <Text style={styles.mutedText}>Loading alternatives...</Text>
        </View>
      ) : null}

      {errorMessage ? (
        <View style={styles.errorBox}>
          <Text style={styles.errorText}>{errorMessage}</Text>
        </View>
      ) : null}

      {!isLoading && !errorMessage && response && alternatives.length === 0 ? (
        <Text style={styles.mutedText}>No alternatives found for this meal.</Text>
      ) : null}

      {currentAlternative ? (
        <AlternativeCard
          alternative={currentAlternative}
          canRequestReplacement={canRequestReplacement}
          hasMultipleAlternatives={alternatives.length > 1}
          isApplying={applyingRecipeId === currentAlternative.recipe_id}
          isPreviewing={previewingRecipeId === currentAlternative.recipe_id}
          onApply={applyReplacement}
          onPreview={() => previewReplacement(currentAlternative)}
          onShuffle={showNextAlternative}
          previewResponse={
            previewResponse && previewedRecipeId === currentAlternative.recipe_id
              ? previewResponse
              : null
          }
        />
      ) : null}

      {successMessage ? <Text style={styles.successText}>{successMessage}</Text> : null}
    </View>
  );
}

function AlternativeCard({
  alternative,
  canRequestReplacement,
  hasMultipleAlternatives,
  isApplying,
  isPreviewing,
  onApply,
  onPreview,
  onShuffle,
  previewResponse,
}: {
  alternative: RecipeAlternativeItem;
  canRequestReplacement: boolean;
  hasMultipleAlternatives: boolean;
  isApplying: boolean;
  isPreviewing: boolean;
  onApply: () => void;
  onPreview: () => void;
  onShuffle: () => void;
  previewResponse: MealReplacementResponse | null;
}) {
  const name = stringValue(alternative.display_name) ?? alternative.recipe_id;
  const replacement = previewResponse?.replacement ?? {};
  const currentMeal = replacement.current_meal ?? {};
  const alternativeMeal = replacement.alternative_meal ?? {};
  const impact = previewResponse?.impact ?? {};
  const hasPreview = Boolean(previewResponse);
  const replaceDisabled =
    isApplying ||
    isPreviewing ||
    !canRequestReplacement ||
    (hasPreview && !previewResponse?.replacement_allowed);

  return (
    <View style={styles.alternativeCard}>
      <View style={styles.alternativeHeader}>
        <View style={styles.alternativeNameBlock}>
          <Text style={styles.alternativeName}>{name}</Text>
          <Text style={styles.alternativeMeta}>
            Similarity {formatScore(alternative.similarity_score)}
          </Text>
        </View>
        <Pressable
          accessibilityRole="button"
          disabled={!hasMultipleAlternatives}
          onPress={onShuffle}
          style={({ pressed }) => [
            styles.shuffleButton,
            pressed && hasMultipleAlternatives ? styles.buttonPressed : null,
            !hasMultipleAlternatives ? styles.buttonDisabled : null,
          ]}
        >
          <Text style={styles.shuffleButtonText}>Shuffle</Text>
        </Pressable>
      </View>

      <Text style={styles.deltaText}>{formatMacroDelta(alternative.macro_delta)}</Text>
      <Text style={styles.deltaText}>
        Time delta {formatSignedNumber(alternative.time_delta_min)} min
      </Text>

      {hasPreview ? (
        <View style={styles.previewBox}>
          <MealSummary title="Current meal" meal={currentMeal} />
          <MealSummary title="Alternative meal" meal={alternativeMeal} />
          <Text style={styles.deltaText}>
            Meal delta {formatMacroDelta(impact.meal_macro_delta)}
          </Text>
          <Text style={styles.deltaText}>
            Day delta {formatMacroDelta(impact.day_totals_delta)}
          </Text>
        </View>
      ) : null}

      {hasPreview && !previewResponse?.replacement_allowed ? (
        <Text style={styles.mutedText}>This alternative cannot replace the meal yet.</Text>
      ) : null}

      <Pressable
        accessibilityRole="button"
        disabled={replaceDisabled}
        onPress={hasPreview ? onApply : onPreview}
        style={({ pressed }) => [
          hasPreview ? styles.replaceButton : styles.previewButton,
          pressed && !replaceDisabled ? styles.buttonPressed : null,
          replaceDisabled ? styles.buttonDisabled : null,
        ]}
      >
        {isPreviewing || isApplying ? (
          <ActivityIndicator color={hasPreview ? "#FFFFFF" : colors.accent} />
        ) : (
          <Text style={hasPreview ? styles.replaceButtonText : styles.previewButtonText}>
            {hasPreview ? "Replace meal" : "Preview changes"}
          </Text>
        )}
      </Pressable>
    </View>
  );
}

function MealSummary({
  title,
  meal,
}: {
  title: string;
  meal: Record<string, unknown>;
}) {
  const name =
    stringValue(meal.display_name) ?? stringValue(meal.recipe) ?? stringValue(meal.recipe_id) ?? "-";
  return (
    <View style={styles.mealSummary}>
      <Text style={styles.mealSummaryTitle}>{title}</Text>
      <Text style={styles.mealSummaryName}>{name}</Text>
      <Text style={styles.deltaText}>
        {formatNumber(meal.kcal)} kcal | {formatNumber(meal.protein_g)}g protein
      </Text>
    </View>
  );
}

function formatSummaryNumber(summary: Record<string, unknown> | undefined, key: string): string {
  const value = summary?.[key];
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "-";
}

function formatMacroDelta(delta: Record<string, unknown> | undefined): string {
  const parts = [
    ["kcal", "kcal"],
    ["protein_g", "protein"],
    ["carbs_g", "carbs"],
    ["fat_g", "fat"],
  ]
    .map(([key, label]) => {
      const value = delta?.[key];
      if (typeof value !== "number" || !Number.isFinite(value)) {
        return "";
      }
      return `${label} ${formatSignedNumber(value)}`;
    })
    .filter(Boolean);
  return parts.length ? parts.join(" | ") : "Macro delta unavailable";
}

function formatScore(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return `${Math.round(value * 100)}%`;
}

function formatSignedNumber(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  const rounded = Math.round(value * 10) / 10;
  return rounded > 0 ? `+${rounded}` : String(rounded);
}

function formatNumber(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return String(Math.round(value));
}

function normalizeTextList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(";")
      .flatMap((item) => item.split(","))
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

const styles = StyleSheet.create({
  actionRow: {
    flexDirection: "row",
    gap: 8,
  },
  alternativeCard: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 8,
    padding: 10,
  },
  alternativeHeader: {
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  alternativeMeta: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "700",
  },
  alternativeName: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
  },
  alternativeNameBlock: {
    flex: 1,
    gap: 2,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  buttonPressed: {
    opacity: 0.82,
  },
  cancelButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 42,
    paddingHorizontal: 10,
  },
  cancelButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "800",
  },
  container: {
    backgroundColor: colors.background,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 10,
    padding: 10,
  },
  deltaText: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "700",
  },
  errorBox: {
    backgroundColor: "#FFF1F1",
    borderColor: "#F3B4B4",
    borderRadius: 8,
    borderWidth: 1,
    padding: 10,
  },
  errorText: {
    color: colors.danger,
    fontSize: 13,
    fontWeight: "700",
  },
  loadingRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  mutedText: {
    color: colors.mutedSoft,
    fontSize: 13,
    fontWeight: "600",
  },
  mealSummary: {
    gap: 3,
  },
  mealSummaryName: {
    color: colors.text,
    fontSize: 13,
    fontWeight: "800",
  },
  mealSummaryTitle: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  previewBox: {
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 8,
    padding: 10,
  },
  previewButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 38,
    paddingHorizontal: 10,
  },
  previewButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "800",
  },
  previewTitle: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
  },
  replaceButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    flex: 1,
    justifyContent: "center",
    minHeight: 42,
    paddingHorizontal: 10,
  },
  replaceButtonText: {
    color: "#FFFFFF",
    fontSize: 13,
    fontWeight: "800",
  },
  shuffleButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 36,
    paddingHorizontal: 10,
  },
  shuffleButtonText: {
    color: colors.accent,
    fontSize: 12,
    fontWeight: "800",
  },
  statusApproved: {
    backgroundColor: "#E8F5EE",
    color: colors.success,
  },
  statusBadge: {
    alignSelf: "flex-start",
    backgroundColor: "#FFF8ED",
    borderRadius: 6,
    color: "#7A4B00",
    flexShrink: 0,
    fontSize: 11,
    fontWeight: "800",
    paddingHorizontal: 8,
    paddingVertical: 4,
    textTransform: "uppercase",
  },
  summaryRow: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  summaryText: {
    color: "#374151",
    fontSize: 12,
    fontWeight: "800",
  },
  successText: {
    color: colors.success,
    fontSize: 13,
    fontWeight: "800",
  },
  warningBox: {
    backgroundColor: "#FFF8ED",
    borderColor: "#F4C790",
    borderRadius: 8,
    borderWidth: 1,
    gap: 4,
    padding: 8,
  },
  warningText: {
    color: "#7A4B00",
    fontSize: 12,
    fontWeight: "700",
  },
});
