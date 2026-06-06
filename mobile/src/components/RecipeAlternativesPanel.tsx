import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, StyleSheet, Text, View } from "react-native";

import { getRecipeAlternatives } from "../services/apiClient";
import type { RecipeAlternativeItem, RecipeAlternativesResponse } from "../types/api";

type RecipeAlternativesPanelProps = {
  sourceRecipeId: string;
  slot?: string;
  datasetProfile?: string;
  householdId?: string;
  memberProfileId?: string;
  memberProfile?: Record<string, unknown>;
  isVisible?: boolean;
};

const DEFAULT_DATASET_PROFILE = "v1_2_demo_final";

export function RecipeAlternativesPanel({
  sourceRecipeId,
  slot,
  datasetProfile = DEFAULT_DATASET_PROFILE,
  householdId,
  memberProfileId,
  memberProfile,
  isVisible = true,
}: RecipeAlternativesPanelProps) {
  const [response, setResponse] = useState<RecipeAlternativesResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [hasLoaded, setHasLoaded] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    setResponse(null);
    setErrorMessage("");
    setHasLoaded(false);
  }, [datasetProfile, householdId, memberProfile, memberProfileId, slot, sourceRecipeId]);

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

  if (!isVisible) {
    return null;
  }

  return (
    <View style={styles.container}>
      <Text style={styles.notice}>
        Read-only alternatives. Replacement is not implemented yet.
      </Text>

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
        <AlternativeCard key={alternative.recipe_id} alternative={alternative} />
      ))}

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

function AlternativeCard({ alternative }: { alternative: RecipeAlternativeItem }) {
  const name = stringValue(alternative.display_name) ?? alternative.recipe_id;
  const status = stringValue(alternative.approval_status) ?? "unknown";
  const whySimilar = normalizeTextList(alternative.why_similar);
  const warnings = normalizeTextList(alternative.warnings);

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
