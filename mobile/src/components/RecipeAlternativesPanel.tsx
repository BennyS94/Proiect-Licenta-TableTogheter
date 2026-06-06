import { Fragment, useEffect, useMemo, useState } from "react";
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
  const [successMessage, setSuccessMessage] = useState("");

  useEffect(() => {
    setResponse(null);
    setErrorMessage("");
    setHasLoaded(false);
    setPreviewResponse(null);
    setPreviewedRecipeId("");
    setPreviewingRecipeId("");
    setApplyingRecipeId("");
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
  const responseWarnings = normalizeTextList(response?.warnings);
  const canRequestReplacement = Boolean(planId && slot && sourceRecipeId);

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

  if (!isVisible) {
    return null;
  }

  return (
    <View style={styles.container}>
      <Text style={styles.notice}>
        Alternatives are read-only until you preview and confirm a replacement.
      </Text>
      {!canRequestReplacement ? (
        <Text style={styles.mutedText}>Replacement preview needs a persisted plan id.</Text>
      ) : null}

      {isLoading ? (
        <View style={styles.loadingRow}>
          <ActivityIndicator color="#165D77" />
          <Text style={styles.mutedText}>Loading alternatives...</Text>
        </View>
      ) : null}

      {errorMessage ? (
        <View style={styles.errorBox}>
          <Text style={styles.errorText}>{errorMessage}</Text>
        </View>
      ) : null}

      {!isLoading && !errorMessage && response ? (
        <View style={styles.summaryRow}>
          <Text style={styles.summaryText}>
            Approved {formatSummaryNumber(response.summary, "approved_count")} | Review{" "}
            {formatSummaryNumber(response.summary, "review_count")}
          </Text>
          <Text style={styles.summaryText}>
            Pool {formatSummaryNumber(response.summary, "candidate_count")}
          </Text>
        </View>
      ) : null}

      {!isLoading && !errorMessage && response && alternatives.length === 0 ? (
        <Text style={styles.mutedText}>No approved or review alternatives returned.</Text>
      ) : null}

      {alternatives.map((alternative) => (
        <Fragment key={alternative.recipe_id}>
          <AlternativeCard
            alternative={alternative}
            canRequestReplacement={canRequestReplacement}
            isPreviewing={previewingRecipeId === alternative.recipe_id}
            onPreview={() => previewReplacement(alternative)}
          />
          {previewResponse && previewedRecipeId === alternative.recipe_id ? (
            <ReplacementPreview
              isApplying={Boolean(applyingRecipeId)}
              onApply={applyReplacement}
              onCancel={() => {
                setPreviewResponse(null);
                setPreviewedRecipeId("");
              }}
              response={previewResponse}
            />
          ) : null}
        </Fragment>
      ))}

      {successMessage ? <Text style={styles.successText}>{successMessage}</Text> : null}

      {responseWarnings.length ? (
        <View style={styles.warningBox}>
          {responseWarnings.slice(0, 3).map((warning, index) => (
            <Text key={`${warning}-${index}`} style={styles.warningText}>
              {warning}
            </Text>
          ))}
        </View>
      ) : null}
    </View>
  );
}

function AlternativeCard({
  alternative,
  canRequestReplacement,
  isPreviewing,
  onPreview,
}: {
  alternative: RecipeAlternativeItem;
  canRequestReplacement: boolean;
  isPreviewing: boolean;
  onPreview: () => void;
}) {
  const name = stringValue(alternative.display_name) ?? alternative.recipe_id;
  const status = stringValue(alternative.approval_status) ?? "unknown";
  const whySimilar = normalizeTextList(alternative.why_similar);
  const warnings = normalizeTextList(alternative.warnings);
  const isReview = status === "review";

  return (
    <View style={styles.alternativeCard}>
      <View style={styles.alternativeHeader}>
        <View style={styles.alternativeNameBlock}>
          <Text style={styles.alternativeName}>{name}</Text>
          <Text style={styles.alternativeMeta}>
            Similarity {formatScore(alternative.similarity_score)} | {status}
          </Text>
        </View>
        <Text style={[styles.statusBadge, status === "approved" ? styles.statusApproved : null]}>
          {status}
        </Text>
      </View>

      <Text style={styles.deltaText}>{formatMacroDelta(alternative.macro_delta)}</Text>
      <Text style={styles.deltaText}>
        Time delta {formatSignedNumber(alternative.time_delta_min)} min
      </Text>

      {whySimilar.length ? (
        <View style={styles.reasonList}>
          {whySimilar.slice(0, 4).map((reason, index) => (
            <Text key={`${reason}-${index}`} style={styles.reasonText}>
              {reason}
            </Text>
          ))}
        </View>
      ) : null}

      {warnings.length ? (
        <View style={styles.warningBox}>
          {warnings.slice(0, 3).map((warning, index) => (
            <Text key={`${warning}-${index}`} style={styles.warningText}>
              {warning}
            </Text>
          ))}
        </View>
      ) : null}

      {isReview ? (
        <Text style={styles.mutedText}>Review alternatives cannot be applied yet.</Text>
      ) : null}
      <Pressable
        accessibilityRole="button"
        disabled={!canRequestReplacement || isPreviewing}
        onPress={onPreview}
        style={({ pressed }) => [
          styles.previewButton,
          pressed && canRequestReplacement ? styles.buttonPressed : null,
          !canRequestReplacement || isPreviewing ? styles.buttonDisabled : null,
        ]}
      >
        {isPreviewing ? (
          <ActivityIndicator color="#165D77" />
        ) : (
          <Text style={styles.previewButtonText}>Preview replacement</Text>
        )}
      </Pressable>
    </View>
  );
}

function ReplacementPreview({
  response,
  isApplying,
  onApply,
  onCancel,
}: {
  response: MealReplacementResponse;
  isApplying: boolean;
  onApply: () => void;
  onCancel: () => void;
}) {
  const replacement = response.replacement ?? {};
  const currentMeal = replacement.current_meal ?? {};
  const alternativeMeal = replacement.alternative_meal ?? {};
  const impact = response.impact ?? {};
  const warnings = normalizeTextList(impact.warnings ?? response.warnings);
  const replaceDisabled = !response.replacement_allowed || isApplying;

  return (
    <View style={styles.previewBox}>
      <Text style={styles.previewTitle}>Replacement preview</Text>
      <MealSummary title="Current meal" meal={currentMeal} />
      <MealSummary title="Alternative meal" meal={alternativeMeal} />
      <Text style={styles.deltaText}>
        Meal delta {formatMacroDelta(impact.meal_macro_delta)}
      </Text>
      <Text style={styles.deltaText}>
        Day delta {formatMacroDelta(impact.day_totals_delta)}
      </Text>
      {warnings.length ? (
        <View style={styles.warningBox}>
          {warnings.slice(0, 3).map((warning, index) => (
            <Text key={`${warning}-${index}`} style={styles.warningText}>
              {warning}
            </Text>
          ))}
        </View>
      ) : null}
      {!response.replacement_allowed ? (
        <Text style={styles.mutedText}>Only approved alternatives can be applied.</Text>
      ) : null}
      <View style={styles.actionRow}>
        <Pressable
          accessibilityRole="button"
          disabled={isApplying}
          onPress={onCancel}
          style={({ pressed }) => [
            styles.cancelButton,
            pressed && !isApplying ? styles.buttonPressed : null,
            isApplying ? styles.buttonDisabled : null,
          ]}
        >
          <Text style={styles.cancelButtonText}>Cancel</Text>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          disabled={replaceDisabled}
          onPress={onApply}
          style={({ pressed }) => [
            styles.replaceButton,
            pressed && !replaceDisabled ? styles.buttonPressed : null,
            replaceDisabled ? styles.buttonDisabled : null,
          ]}
        >
          {isApplying ? (
            <ActivityIndicator color="#FFFFFF" />
          ) : (
            <Text style={styles.replaceButtonText}>Replace meal</Text>
          )}
        </Pressable>
      </View>
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
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
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
    color: "#4B5563",
    fontSize: 12,
    fontWeight: "700",
  },
  alternativeName: {
    color: "#111827",
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
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 42,
    paddingHorizontal: 10,
  },
  cancelButtonText: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
  },
  container: {
    backgroundColor: "#F8FAFC",
    borderColor: "#CBD5E1",
    borderRadius: 8,
    borderWidth: 1,
    gap: 10,
    padding: 10,
  },
  deltaText: {
    color: "#374151",
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
    color: "#B42318",
    fontSize: 13,
    fontWeight: "700",
  },
  loadingRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  mutedText: {
    color: "#6B7280",
    fontSize: 13,
    fontWeight: "600",
  },
  notice: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
  },
  mealSummary: {
    gap: 3,
  },
  mealSummaryName: {
    color: "#111827",
    fontSize: 13,
    fontWeight: "800",
  },
  mealSummaryTitle: {
    color: "#6B7280",
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  previewBox: {
    backgroundColor: "#FFFFFF",
    borderColor: "#BFD9E2",
    borderRadius: 8,
    borderWidth: 1,
    gap: 8,
    padding: 10,
  },
  previewButton: {
    alignItems: "center",
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 38,
    paddingHorizontal: 10,
  },
  previewButtonText: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
  },
  previewTitle: {
    color: "#111827",
    fontSize: 14,
    fontWeight: "800",
  },
  replaceButton: {
    alignItems: "center",
    backgroundColor: "#165D77",
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
  reasonList: {
    gap: 3,
  },
  reasonText: {
    color: "#4B5563",
    fontSize: 12,
    fontWeight: "600",
  },
  statusApproved: {
    backgroundColor: "#E8F5EE",
    color: "#1E7A4C",
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
    color: "#1E7A4C",
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
