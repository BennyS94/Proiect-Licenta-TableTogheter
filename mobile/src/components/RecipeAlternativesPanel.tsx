import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, StyleSheet, Text, View } from "react-native";
import Svg, { Path } from "react-native-svg";

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
import { formatRecipeDisplayName } from "../utils/formatRecipeDisplayName";
import { MacroMiniStat } from "./ui/MacroMiniStat";

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
  shouldLoad?: boolean;
  onReplacementApplied?: (response: MealReplacementResponse) => void;
};

export type RecipeAlternativePreviewPrefetchInput = {
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
  onPreviewReady?: () => void;
};

const DEFAULT_DATASET_PROFILE = "current";
const PREVIEW_PREFETCH_TIMEOUT_MS = 15000;
const alternativesResponseCache = new Map<string, RecipeAlternativesResponse>();
const alternativesRequestCache = new Map<string, Promise<RecipeAlternativesResponse>>();
const previewResponseCache = new Map<string, MealReplacementResponse>();
const previewRequestCache = new Map<string, Promise<MealReplacementResponse>>();
const previewFailedCache = new Set<string>();

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
  shouldLoad = isVisible,
  onReplacementApplied,
}: RecipeAlternativesPanelProps) {
  const requestCacheKey = buildAlternativesCacheKey({
    datasetProfile,
    householdId,
    memberProfile,
    memberProfileId,
    slot,
    sourceRecipeId,
  });
  const cachedResponse = requestCacheKey
    ? alternativesResponseCache.get(requestCacheKey) ?? null
    : null;
  const [response, setResponse] = useState<RecipeAlternativesResponse | null>(cachedResponse);
  const [errorMessage, setErrorMessage] = useState("");
  const [hasLoaded, setHasLoaded] = useState(Boolean(cachedResponse));
  const [isLoading, setIsLoading] = useState(false);
  const [previewResponse, setPreviewResponse] = useState<MealReplacementResponse | null>(null);
  const [previewedRecipeId, setPreviewedRecipeId] = useState("");
  const [previewingRecipeId, setPreviewingRecipeId] = useState("");
  const [previewCacheVersion, setPreviewCacheVersion] = useState(0);
  const [applyingRecipeId, setApplyingRecipeId] = useState("");
  const [successMessage, setSuccessMessage] = useState("");

  useEffect(() => {
    const cached = requestCacheKey
      ? alternativesResponseCache.get(requestCacheKey) ?? null
      : null;
    setErrorMessage("");
    setResponse(cached);
    setHasLoaded(Boolean(cached));
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
    requestCacheKey,
    slot,
    sourceRecipeId,
  ]);

  useEffect(() => {
    let isCurrent = true;

    async function loadAlternatives() {
      if (!shouldLoad || hasLoaded || !sourceRecipeId || !requestCacheKey) {
        return;
      }

      setIsLoading(true);
      setErrorMessage("");
      try {
        const cached = alternativesResponseCache.get(requestCacheKey);
        const payload =
          cached ??
          (await getOrCreateAlternativesRequest(requestCacheKey, {
            recipe_id: sourceRecipeId,
            slot,
            top_k: 5,
            candidate_pool_k: 20,
            dataset_profile: datasetProfile,
            household_id: householdId,
            member_profile_id: memberProfileId,
            member_profile: memberProfile,
            feedback_enabled: true,
            approval_mode: "approved_only",
          }));
        if (isCurrent) {
          alternativesResponseCache.set(requestCacheKey, payload);
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
    memberProfile,
    memberProfileId,
    requestCacheKey,
    shouldLoad,
    slot,
    sourceRecipeId,
  ]);

  const alternatives = useMemo(
    () => filterDisplayableAlternatives(response?.alternatives),
    [response],
  );
  const canRequestReplacement = Boolean(planId && slot && sourceRecipeId);

  useEffect(() => {
    if (
      !shouldLoad ||
      !isVisible ||
      !canRequestReplacement ||
      !planId ||
      !slot ||
      !alternatives.length
    ) {
      return;
    }

    let isCurrent = true;

    async function prefetchPreviews() {
      await prefetchRecipeAlternativePreviews({
        datasetProfile,
        dayIndex,
        generationType,
        householdId,
        memberId,
        memberProfile,
        memberProfileId,
        planId,
        replaceScope,
        slot,
        sourceRecipeId,
        onPreviewReady: () => {
          if (isCurrent) {
            setPreviewCacheVersion((version) => version + 1);
          }
        },
      });
      if (isCurrent) {
        setPreviewCacheVersion((version) => version + 1);
      }
    }

    void prefetchPreviews();

    return () => {
      isCurrent = false;
    };
  }, [
    alternatives,
    canRequestReplacement,
    datasetProfile,
    dayIndex,
    generationType,
    householdId,
    isVisible,
    memberId,
    memberProfile,
    memberProfileId,
    planId,
    replaceScope,
    shouldLoad,
    slot,
    sourceRecipeId,
  ]);

  async function previewReplacement(alternative: RecipeAlternativeItem) {
    if (!planId || !slot) {
      setErrorMessage("Generate and save a plan before previewing replacement.");
      return;
    }

    const previewCacheKey = buildPreviewCacheKey({
      alternativeRecipeId: alternative.recipe_id,
      datasetProfile,
      dayIndex,
      generationType,
      householdId,
      memberId,
      memberProfile,
      memberProfileId,
      planId,
      replaceScope,
      slot,
      sourceRecipeId,
    });
    const cachedPreview = previewResponseCache.get(previewCacheKey);
    if (cachedPreview) {
      setPreviewResponse(cachedPreview);
      setPreviewedRecipeId(alternative.recipe_id);
      setPreviewingRecipeId("");
      setSuccessMessage("");
      setErrorMessage("");
      return;
    }

    const request = buildReplacementRequest(alternative.recipe_id);
    setPreviewingRecipeId(alternative.recipe_id);
    setPreviewResponse(null);
    setPreviewedRecipeId("");
    setSuccessMessage("");
    setErrorMessage("");
    previewFailedCache.delete(previewCacheKey);

    try {
      const payload = await getOrCreatePreviewRequest(previewCacheKey, () =>
        previewMealReplacement(planId, request, PREVIEW_PREFETCH_TIMEOUT_MS),
      );
      if (isUsablePreviewResponse(payload)) {
        previewResponseCache.set(previewCacheKey, payload);
        setPreviewCacheVersion((version) => version + 1);
      }
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

      {alternatives.length ? (
        <View style={styles.alternativesList}>
          {alternatives.map((alternative) => {
            const previewCacheKey = buildPreviewCacheKey({
              alternativeRecipeId: alternative.recipe_id,
              datasetProfile,
              dayIndex,
              generationType,
              householdId,
              memberId,
              memberProfile,
              memberProfileId,
              planId,
              replaceScope,
              slot,
              sourceRecipeId,
            });
            const preparedPreviewResponse =
              previewCacheVersion >= 0
                ? previewResponseCache.get(previewCacheKey) ?? null
                : null;

            return (
              <AlternativeCard
                alternative={alternative}
                canRequestReplacement={canRequestReplacement}
                isApplying={applyingRecipeId === alternative.recipe_id}
                isPreviewing={previewingRecipeId === alternative.recipe_id}
                key={alternative.recipe_id}
                onApply={applyReplacement}
                onPreview={() => previewReplacement(alternative)}
                preparedPreviewResponse={preparedPreviewResponse}
                previewResponse={
                  previewResponse && previewedRecipeId === alternative.recipe_id
                    ? previewResponse
                    : null
                }
                sourceRecipe={response?.source_recipe}
              />
            );
          })}
        </View>
      ) : null}

      {successMessage ? <Text style={styles.successText}>{successMessage}</Text> : null}
    </View>
  );
}

export async function prefetchRecipeAlternativePreviews({
  sourceRecipeId,
  slot,
  dayIndex = 1,
  datasetProfile = DEFAULT_DATASET_PROFILE,
  generationType = "individual",
  householdId,
  memberId,
  memberProfile,
  memberProfileId,
  onPreviewReady,
  planId,
  replaceScope,
}: RecipeAlternativePreviewPrefetchInput): Promise<void> {
  if (!planId || !slot || !sourceRecipeId) {
    return;
  }

  const alternativesCacheKey = buildAlternativesCacheKey({
    datasetProfile,
    householdId,
    memberProfile,
    memberProfileId,
    slot,
    sourceRecipeId,
  });
  const alternativesPayload =
    alternativesResponseCache.get(alternativesCacheKey) ??
    (await getOrCreateAlternativesRequest(alternativesCacheKey, {
      recipe_id: sourceRecipeId,
      slot,
      top_k: 5,
      candidate_pool_k: 20,
      dataset_profile: datasetProfile,
      household_id: householdId,
      member_profile_id: memberProfileId,
      member_profile: memberProfile,
      feedback_enabled: true,
      approval_mode: "approved_only",
    }));
  alternativesResponseCache.set(alternativesCacheKey, alternativesPayload);

  for (const alternative of filterDisplayableAlternatives(alternativesPayload.alternatives)) {
    const previewCacheKey = buildPreviewCacheKey({
      alternativeRecipeId: alternative.recipe_id,
      datasetProfile,
      dayIndex,
      generationType,
      householdId,
      memberId,
      memberProfile,
      memberProfileId,
      planId,
      replaceScope,
      slot,
      sourceRecipeId,
    });

    if (
      previewResponseCache.has(previewCacheKey) ||
      previewFailedCache.has(previewCacheKey)
    ) {
      continue;
    }

    try {
      const payload = await getOrCreatePreviewRequest(previewCacheKey, () =>
        previewMealReplacement(
          planId,
          buildReplacementRequestPayload({
            alternativeRecipeId: alternative.recipe_id,
            datasetProfile,
            dayIndex,
            generationType,
            memberId,
            memberProfileId,
            replaceScope,
            slot,
            sourceRecipeId,
          }),
          PREVIEW_PREFETCH_TIMEOUT_MS,
        ),
      );
      if (isUsablePreviewResponse(payload)) {
        previewResponseCache.set(previewCacheKey, payload);
        previewFailedCache.delete(previewCacheKey);
        onPreviewReady?.();
      } else {
        previewFailedCache.add(previewCacheKey);
      }
    } catch {
      previewFailedCache.add(previewCacheKey);
    }
  }
}

function getOrCreateAlternativesRequest(
  cacheKey: string,
  request: Parameters<typeof getRecipeAlternatives>[0],
): Promise<RecipeAlternativesResponse> {
  const existingRequest = alternativesRequestCache.get(cacheKey);
  if (existingRequest) {
    return existingRequest;
  }

  const nextRequest = getRecipeAlternatives(request).finally(() => {
    alternativesRequestCache.delete(cacheKey);
  });
  alternativesRequestCache.set(cacheKey, nextRequest);
  return nextRequest;
}

function getOrCreatePreviewRequest(
  cacheKey: string,
  createRequest: () => Promise<MealReplacementResponse>,
): Promise<MealReplacementResponse> {
  const cached = previewResponseCache.get(cacheKey);
  if (cached) {
    return Promise.resolve(cached);
  }

  const existingRequest = previewRequestCache.get(cacheKey);
  if (existingRequest) {
    return existingRequest;
  }

  const nextRequest = createRequest().finally(() => {
    previewRequestCache.delete(cacheKey);
  });
  previewRequestCache.set(cacheKey, nextRequest);
  return nextRequest;
}

function buildAlternativesCacheKey({
  datasetProfile,
  householdId,
  memberProfile,
  memberProfileId,
  slot,
  sourceRecipeId,
}: {
  datasetProfile?: string;
  householdId?: string;
  memberProfile?: Record<string, unknown>;
  memberProfileId?: string;
  slot?: string;
  sourceRecipeId?: string;
}): string {
  return JSON.stringify({
    datasetProfile: datasetProfile ?? DEFAULT_DATASET_PROFILE,
    householdId: householdId ?? "",
    memberProfile: memberProfile ?? null,
    memberProfileId: memberProfileId ?? "",
    slot: slot ?? "",
    sourceRecipeId: sourceRecipeId ?? "",
  });
}

function buildPreviewCacheKey({
  alternativeRecipeId,
  datasetProfile,
  dayIndex,
  generationType,
  householdId,
  memberId,
  memberProfile,
  memberProfileId,
  planId,
  replaceScope,
  slot,
  sourceRecipeId,
}: {
  alternativeRecipeId?: string;
  datasetProfile?: string;
  dayIndex?: number;
  generationType?: string;
  householdId?: string;
  memberId?: string;
  memberProfile?: Record<string, unknown>;
  memberProfileId?: string;
  planId?: string;
  replaceScope?: string;
  slot?: string;
  sourceRecipeId?: string;
}): string {
  return JSON.stringify({
    alternativeRecipeId: alternativeRecipeId ?? "",
    datasetProfile: datasetProfile ?? DEFAULT_DATASET_PROFILE,
    dayIndex: dayIndex ?? 1,
    generationType: generationType ?? "individual",
    householdId: householdId ?? "",
    memberId: memberId ?? "",
    memberProfile: memberProfile ?? null,
    memberProfileId: memberProfileId ?? "",
    planId: planId ?? "",
    replaceScope: replaceScope ?? "",
    slot: slot ?? "",
    sourceRecipeId: sourceRecipeId ?? "",
  });
}

function isUsablePreviewResponse(response: MealReplacementResponse): boolean {
  return Boolean(response.replacement?.alternative_meal);
}

function filterDisplayableAlternatives(
  alternatives: RecipeAlternativeItem[] | undefined,
): RecipeAlternativeItem[] {
  return (alternatives ?? []).filter((alternative) => {
    const status = stringValue(alternative.approval_status);
    return status === "approved";
  });
}

function buildReplacementRequestPayload({
  alternativeRecipeId,
  datasetProfile,
  dayIndex,
  generationType,
  memberId,
  memberProfileId,
  replaceScope,
  slot,
  sourceRecipeId,
}: {
  alternativeRecipeId: string;
  datasetProfile: string;
  dayIndex: number;
  generationType: "individual" | "household";
  memberId?: string;
  memberProfileId?: string;
  replaceScope?: MealReplacementScope;
  slot: string;
  sourceRecipeId: string;
}): MealReplacementRequest {
  return {
    day_index: dayIndex,
    slot,
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

function AlternativeCard({
  alternative,
  canRequestReplacement,
  isApplying,
  isPreviewing,
  onApply,
  onPreview,
  preparedPreviewResponse,
  previewResponse,
  sourceRecipe,
}: {
  alternative: RecipeAlternativeItem;
  canRequestReplacement: boolean;
  isApplying: boolean;
  isPreviewing: boolean;
  onApply: () => void;
  onPreview: () => void;
  preparedPreviewResponse: MealReplacementResponse | null;
  previewResponse: MealReplacementResponse | null;
  sourceRecipe?: Record<string, unknown>;
}) {
  const name = formatRecipeDisplayName(
    stringValue(alternative.display_name) ?? alternative.recipe_id,
  );
  const replacement = previewResponse?.replacement ?? {};
  const currentMeal = replacement.current_meal ?? {};
  const impact = previewResponse?.impact ?? {};
  const hasPreview = Boolean(previewResponse);
  const preparedAlternativeMeal =
    (previewResponse ?? preparedPreviewResponse)?.replacement?.alternative_meal ?? {};
  const alternativeMacros = previewResponse || preparedPreviewResponse
    ? {
        carbs_g: getMacroNumber(preparedAlternativeMeal, "carbs_g"),
        fat_g: getMacroNumber(preparedAlternativeMeal, "fat_g"),
        kcal: getMacroNumber(preparedAlternativeMeal, "kcal"),
        protein_g: getMacroNumber(preparedAlternativeMeal, "protein_g"),
      }
    : undefined;
  const currentName = formatRecipeDisplayName(
    getMealName(currentMeal) ?? stringValue(sourceRecipe?.display_name) ?? "Current meal",
  );
  const replaceDisabled =
    isApplying ||
    isPreviewing ||
    !canRequestReplacement ||
    (hasPreview && !previewResponse?.replacement_allowed);
  const buttonDisabled = replaceDisabled;
  const buttonShowsReplacement = hasPreview;

  return (
    <View style={styles.alternativeCard}>
      <View style={styles.alternativeHeader}>
        <Text numberOfLines={2} style={styles.alternativeName}>
          {name}
        </Text>
        <View style={styles.similarityBadge}>
          <Text style={styles.similarityBadgeText}>
            Similar {formatScore(alternative.similarity_score)}
          </Text>
        </View>
      </View>

      <MacroIconRow macros={alternativeMacros} />

      {hasPreview ? (
        <View style={styles.comparisonBox}>
          <MealComparisonBlock label="Current" meal={currentMeal} name={currentName} />
          <ImpactSummary delta={asRecord(impact.meal_macro_delta)} timeDelta={alternative.time_delta_min} />
        </View>
      ) : null}

      {hasPreview && !previewResponse?.replacement_allowed ? (
        <Text style={styles.mutedText}>This alternative cannot replace the meal yet.</Text>
      ) : null}

      <Pressable
        accessibilityRole="button"
        disabled={buttonDisabled}
        onPress={buttonShowsReplacement ? onApply : onPreview}
        style={({ pressed }) => [
          buttonShowsReplacement ? styles.replaceButton : styles.previewButton,
          pressed && !buttonDisabled ? styles.buttonPressed : null,
          buttonDisabled ? styles.buttonDisabled : null,
        ]}
      >
        {isPreviewing || isApplying ? (
          <ActivityIndicator color={buttonShowsReplacement ? "#FFFFFF" : colors.accent} />
        ) : (
          <Text style={buttonShowsReplacement ? styles.replaceButtonText : styles.previewButtonText}>
            {buttonShowsReplacement ? "Replace meal" : "Preview"}
          </Text>
        )}
      </Pressable>
    </View>
  );
}

function MealComparisonBlock({
  label,
  meal,
  name,
}: {
  label: string;
  meal: Record<string, unknown>;
  name: string;
}) {
  return (
    <View style={styles.comparisonBlock}>
      <Text style={styles.comparisonLabel}>{label}</Text>
      <Text style={styles.comparisonName}>{name}</Text>
      <MacroIconRow
        macros={{
          carbs_g: getMacroNumber(meal, "carbs_g"),
          fat_g: getMacroNumber(meal, "fat_g"),
          kcal: getMacroNumber(meal, "kcal"),
          protein_g: getMacroNumber(meal, "protein_g"),
        }}
      />
    </View>
  );
}

function ImpactSummary({
  delta,
  label = "Impact",
  timeDelta,
}: {
  delta: Record<string, unknown> | undefined;
  label?: string;
  timeDelta?: unknown;
}) {
  const time = formatCookingDelta(timeDelta);

  if (!hasMeaningfulMacroDelta(delta) && !time) {
    return null;
  }

  return (
    <View style={styles.impactBlock}>
      <Text style={styles.comparisonLabel}>{label}</Text>
      <MacroIconRow macros={delta} signed />
      {time ? <TimeMiniStat value={time} /> : null}
    </View>
  );
}

function MacroIconRow({
  macros,
  signed = false,
}: {
  macros: Record<string, unknown> | undefined;
  signed?: boolean;
}) {
  const items = [
    { decimals: 0, key: "kcal", kind: "calories" as const, suffix: "" },
    { decimals: 1, key: "protein_g", kind: "protein" as const, suffix: "g" },
    { decimals: 1, key: "carbs_g", kind: "carbs" as const, suffix: "g" },
    { decimals: 1, key: "fat_g", kind: "fat" as const, suffix: "g" },
  ]
    .map((item) => {
      const value = macros?.[item.key];
      const numeric = numberValue(value);
      if (signed && (numeric === null || Math.abs(numeric) < 0.05)) {
        return null;
      }
      const text = signed
        ? formatSignedMacroValue(value, item.decimals, item.suffix)
        : formatMacroValue(value, item.decimals, item.suffix);
      return text ? { ...item, text } : null;
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  if (!items.length) {
    return null;
  }

  return (
    <View style={styles.macroIconRow}>
      {items.map((item) => (
        <MacroMiniStat
          iconSize={16}
          key={item.key}
          kind={item.kind}
          tone="soft"
          value={item.text}
          valueSize={12}
          valueWeight="700"
        />
      ))}
    </View>
  );
}

function TimeMiniStat({ value }: { value: string }) {
  return (
    <View style={styles.timeMiniStat}>
      <ClockMiniIcon color={colors.mutedSoft} size={17} />
      <Text numberOfLines={1} style={styles.timeMiniStatText}>
        {value}
      </Text>
    </View>
  );
}

function ClockMiniIcon({ color, size }: { color: string; size: number }) {
  return (
    <Svg height={size} viewBox="0 0 24 24" width={size}>
      <Path
        d="M12 5.1a6.9 6.9 0 1 0 0 13.8 6.9 6.9 0 0 0 0-13.8Zm0 1.8a5.1 5.1 0 1 1 0 10.2 5.1 5.1 0 0 1 0-10.2Zm.75 2.4h-1.5v3.55l3.1 1.85.76-1.25-2.36-1.4V9.3Z"
        fill={color}
      />
    </Svg>
  );
}

function formatScore(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return `${Math.round(value * 100)}%`;
}

function formatMacroValue(value: unknown, decimals: number, suffix: string): string {
  const numeric = numberValue(value);
  if (numeric === null) {
    return "";
  }
  return `${formatRoundedNumber(numeric, decimals)}${suffix}`;
}

function formatSignedMacroValue(value: unknown, decimals: number, suffix: string): string {
  const numeric = numberValue(value);
  if (numeric === null) {
    return "";
  }
  const rounded = roundTo(numeric, decimals);
  const prefix = rounded > 0 ? "+" : "";
  return `${prefix}${formatRoundedNumber(rounded, decimals)}${suffix}`;
}

function formatSignedMetric(
  value: unknown,
  options: {
    decimals: number;
    suffix: string;
  },
): string {
  const numeric = numberValue(value);
  if (numeric === null) {
    return "";
  }
  const rounded = roundTo(numeric, options.decimals);
  const prefix = rounded > 0 ? "+" : "";
  return `${prefix}${formatRoundedNumber(rounded, options.decimals)}${getMetricSuffixSeparator("", options.suffix)}${options.suffix}`;
}

function formatCookingDelta(value: unknown): string {
  const time = formatSignedMetric(value, { decimals: 0, suffix: "min" });
  return time ? `Cooking ${time}` : "";
}

function getMetricSuffixSeparator(prefix: string, suffix: string): string {
  if ((prefix && suffix === "g") || suffix.startsWith("g ")) {
    return "";
  }
  return " ";
}

function getMacroNumber(record: Record<string, unknown> | undefined, key: string): number | null {
  if (!record) {
    return null;
  }
  const candidateKeys =
    key === "kcal"
      ? ["kcal", "calories", "kcal_per_serving"]
      : [key, key.replace("_g", ""), `${key}_per_serving`];

  for (const candidateKey of candidateKeys) {
    const value = numberValue(record[candidateKey]);
    if (value !== null) {
      return value;
    }
  }

  return null;
}

function getMealName(meal: Record<string, unknown> | undefined): string | null {
  return (
    stringValue(meal?.display_name) ??
    stringValue(meal?.recipe) ??
    stringValue(meal?.recipe_name) ??
    stringValue(meal?.recipe_id)
  );
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  return value as Record<string, unknown>;
}

function hasMeaningfulMacroDelta(delta: Record<string, unknown> | undefined): boolean {
  if (!delta) {
    return false;
  }
  return ["kcal", "protein_g", "carbs_g", "fat_g"].some((key) => {
    const value = numberValue(delta[key]);
    return value !== null && Math.abs(value) >= 0.05;
  });
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function roundTo(value: number, decimals: number): number {
  const multiplier = 10 ** decimals;
  return Math.round(value * multiplier) / multiplier;
}

function formatRoundedNumber(value: number, decimals: number): string {
  const rounded = roundTo(value, decimals);
  if (Number.isInteger(rounded)) {
    return String(rounded);
  }
  return rounded.toFixed(decimals);
}

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

const styles = StyleSheet.create({
  alternativesList: {
    gap: 8,
  },
  alternativeCard: {
    backgroundColor: colors.card,
    borderColor: "#DDEAD3",
    borderRadius: 8,
    borderWidth: 1,
    gap: 7,
    padding: 10,
  },
  alternativeHeader: {
    alignItems: "flex-start",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  alternativeName: {
    color: colors.text,
    flex: 1,
    fontSize: 15,
    fontWeight: "800",
    lineHeight: 19,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  buttonPressed: {
    opacity: 0.82,
  },
  container: {
    backgroundColor: colors.background,
    borderColor: "transparent",
    borderRadius: 8,
    gap: 10,
    paddingVertical: 4,
  },
  comparisonBlock: {
    gap: 4,
  },
  comparisonBox: {
    backgroundColor: "#FBFDF7",
    borderColor: "#E3EAD8",
    borderRadius: 8,
    borderWidth: 1,
    gap: 7,
    padding: 7,
  },
  comparisonLabel: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
  },
  comparisonName: {
    color: colors.text,
    fontSize: 13,
    fontWeight: "800",
    lineHeight: 17,
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
  previewButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 34,
    paddingHorizontal: 10,
  },
  previewButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "800",
  },
  replaceButton: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    justifyContent: "center",
    minHeight: 42,
    paddingHorizontal: 10,
  },
  replaceButtonText: {
    color: "#FFFFFF",
    fontSize: 13,
    fontWeight: "800",
  },
  impactBlock: {
    gap: 5,
  },
  macroIconRow: {
    alignItems: "center",
    flexDirection: "row",
    flexWrap: "wrap",
    columnGap: 8,
    rowGap: 4,
  },
  similarityBadge: {
    alignItems: "center",
    backgroundColor: "#F1F8E9",
    borderColor: "#DDEAD3",
    borderRadius: 999,
    borderWidth: 1,
    flexShrink: 0,
    justifyContent: "center",
    paddingHorizontal: 7,
    paddingVertical: 2,
  },
  similarityBadgeText: {
    color: colors.accentDark,
    fontSize: 10,
    fontWeight: "800",
  },
  successText: {
    color: colors.success,
    fontSize: 13,
    fontWeight: "800",
  },
  timeMiniStat: {
    alignItems: "center",
    flexDirection: "row",
    gap: 4,
  },
  timeMiniStatText: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: "800",
  },
});
