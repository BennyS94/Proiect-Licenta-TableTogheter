import { useMemo, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";

import { GroceryListSection } from "../components/GroceryListSection";
import { MemberCard } from "../components/MemberCard";
import { PlanDayCard } from "../components/PlanDayCard";
import { StatusCard } from "../components/StatusCard";
import { API_BASE_URL } from "../config/api";
import {
  generateIndividualPlan,
  getDemoHousehold,
  getHealth,
} from "../services/apiClient";
import type {
  DemoHouseholdResponse,
  DemoMemberProfile,
  GroceryListResponse,
  HealthResponse,
  IndividualPlanGenerateRequest,
  IndividualPlanGenerateResponse,
} from "../types/api";

type HealthState = "idle" | "loading" | "connected" | "error";

const GENERATION_OPTIONS = {
  selection_mode: "balanced_day",
  portion_policy: "target_aware",
  meal_realism_mode: "practical",
  quality_gate: "demo_safe",
  profile_guard: "demo",
  multi_day_mode: "global_alternatives_3_day",
  multi_day_no_repeat_policy: "hard",
  day_candidate_builder: "direct_from_slots",
  include_grocery_list: true,
  include_purchase_suggestions: true,
  include_price_estimates: true,
  feedback_enabled: false,
};

export function HomeScreen() {
  const [healthStatus, setHealthStatus] = useState<HealthState>("idle");
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [demoHousehold, setDemoHousehold] = useState<DemoHouseholdResponse | null>(null);
  const [selectedMemberId, setSelectedMemberId] = useState("");
  const [isLoadingHousehold, setIsLoadingHousehold] = useState(false);
  const [isGeneratingPlan, setIsGeneratingPlan] = useState(false);
  const [generatedPlan, setGeneratedPlan] = useState<IndividualPlanGenerateResponse | null>(
    null,
  );
  const [errorMessage, setErrorMessage] = useState("");

  const selectedMember = useMemo(
    () =>
      (demoHousehold?.members ?? []).find(
        (member) => memberKey(member) === selectedMemberId,
      ) ?? null,
    [demoHousehold, selectedMemberId],
  );
  const groceryList = generatedPlan ? getGroceryListFromPlanResponse(generatedPlan) : null;

  async function checkBackendHealth() {
    setHealthStatus("loading");
    setErrorMessage("");

    try {
      const response = await getHealth();
      setHealth(response);
      setHealthStatus("connected");
    } catch (error) {
      setHealth(null);
      setErrorMessage(error instanceof Error ? error.message : "Unknown error");
      setHealthStatus("error");
    }
  }

  async function loadDemoHousehold() {
    setIsLoadingHousehold(true);
    setErrorMessage("");
    setGeneratedPlan(null);

    try {
      const household = await getDemoHousehold();
      setDemoHousehold(household);
      setSelectedMemberId("");
    } catch (error) {
      setDemoHousehold(null);
      setSelectedMemberId("");
      setErrorMessage(error instanceof Error ? error.message : "Household fetch failed");
    } finally {
      setIsLoadingHousehold(false);
    }
  }

  async function generatePlanForSelectedMember() {
    if (!selectedMember) {
      setErrorMessage("Select a demo member before generating a plan.");
      return;
    }

    setIsGeneratingPlan(true);
    setErrorMessage("");
    setGeneratedPlan(null);

    try {
      const request = buildGenerateRequest(selectedMember);
      const response = await generateIndividualPlan(request);
      setGeneratedPlan(response);
      if (response.status === "blocked") {
        setErrorMessage("Profile guard blocked generation for this member.");
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : "Generate failed");
    } finally {
      setIsGeneratingPlan(false);
    }
  }

  return (
    <ScrollView contentContainerStyle={styles.container}>
      <View style={styles.header}>
        <Text style={styles.title}>TableTogether</Text>
        <Text style={styles.subtitle}>Mobile MVP</Text>
      </View>

      <View style={styles.section}>
        <Text style={styles.label}>Backend URL</Text>
        <Text style={styles.url}>{API_BASE_URL}</Text>
      </View>

      <ActionButton
        disabled={healthStatus === "loading"}
        loading={healthStatus === "loading"}
        label="Check backend health"
        onPress={checkBackendHealth}
      />

      <StatusCard title="Backend status" status={healthStatus}>
        {healthStatus === "connected" && health ? (
          <>
            <InfoRow label="Service" value={health.service} />
            <InfoRow label="Version" value={health.version} />
            <InfoRow label="Database" value={health.database} />
          </>
        ) : null}
        {healthStatus === "idle" ? (
          <Text style={styles.mutedText}>Run the backend, then check health.</Text>
        ) : null}
      </StatusCard>

      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Demo household</Text>
          <Text style={styles.panelMeta}>
            {demoHousehold ? `${demoHousehold.members.length} members` : "Not loaded"}
          </Text>
        </View>
        <ActionButton
          disabled={isLoadingHousehold}
          loading={isLoadingHousehold}
          label="Load demo household"
          onPress={loadDemoHousehold}
          variant="secondary"
        />
        {demoHousehold ? (
          <View style={styles.memberList}>
            <Text style={styles.householdName}>
              {demoHousehold.household_name ?? demoHousehold.household_id}
            </Text>
            {demoHousehold.members.map((member) => {
              const key = memberKey(member);
              return (
                <MemberCard
                  key={key}
                  member={member}
                  selected={key === selectedMemberId}
                  onPress={() => setSelectedMemberId(key)}
                />
              );
            })}
          </View>
        ) : null}
      </View>

      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Individual plan</Text>
          <Text style={styles.panelMeta}>3 days / grocery</Text>
        </View>
        <ActionButton
          disabled={!selectedMember || isGeneratingPlan}
          loading={isGeneratingPlan}
          label="Generate plan for selected member"
          onPress={generatePlanForSelectedMember}
        />
        {selectedMember ? (
          <Text style={styles.mutedText}>
            Selected: {selectedMember.display_name ?? selectedMember.profile_name}
          </Text>
        ) : (
          <Text style={styles.mutedText}>Select a member to enable generation.</Text>
        )}
      </View>

      {errorMessage ? <Text style={styles.errorText}>{errorMessage}</Text> : null}

      {generatedPlan ? (
        <View style={styles.panel}>
          <View style={styles.panelHeader}>
            <Text style={styles.panelTitle}>Generated plan</Text>
            <Text style={styles.panelMeta}>Status: {generatedPlan.status}</Text>
          </View>
          <InfoRow label="Plan ID" value={generatedPlan.plan_id ?? "missing"} />
          <InfoRow label="Days" value={String(generatedPlan.days ?? "-")} />
          {renderWarnings(generatedPlan.warnings)}
          <View style={styles.dayList}>
            {(generatedPlan.daily_plan ?? []).map((day, index) => (
              <PlanDayCard key={`${day.day_index ?? index}`} day={day} />
            ))}
          </View>
        </View>
      ) : null}

      {generatedPlan ? <GroceryListSection groceryList={groceryList} /> : null}
    </ScrollView>
  );
}

function ActionButton({
  disabled,
  label,
  loading,
  onPress,
  variant = "primary",
}: {
  disabled?: boolean;
  label: string;
  loading?: boolean;
  onPress: () => void;
  variant?: "primary" | "secondary";
}) {
  return (
    <Pressable
      accessibilityRole="button"
      disabled={disabled}
      onPress={onPress}
      style={({ pressed }) => [
        styles.button,
        variant === "secondary" ? styles.buttonSecondary : null,
        pressed && !disabled ? styles.buttonPressed : null,
        disabled ? styles.buttonDisabled : null,
      ]}
    >
      {loading ? (
        <ActivityIndicator color={variant === "secondary" ? "#165D77" : "#FFFFFF"} />
      ) : (
        <Text
          style={[
            styles.buttonText,
            variant === "secondary" ? styles.buttonTextSecondary : null,
          ]}
        >
          {label}
        </Text>
      )}
    </Pressable>
  );
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.infoRow}>
      <Text style={styles.infoLabel}>{label}</Text>
      <Text style={styles.infoValue}>{value}</Text>
    </View>
  );
}

function buildGenerateRequest(member: DemoMemberProfile): IndividualPlanGenerateRequest {
  const profileId = String(member.member_profile_id ?? member.member_id ?? "demo_member");
  return {
    dataset_profile: "v1_2_demo_final",
    days: 3,
    member_profile: {
      ...member,
      member_profile_id: profileId,
      profile_name: member.profile_name ?? member.display_name ?? profileId,
    },
    generation_options: GENERATION_OPTIONS,
    include_grocery_list: true,
    include_purchase_suggestions: true,
    include_price_estimates: true,
    feedback_enabled: false,
  };
}

function getGroceryListFromPlanResponse(
  response: IndividualPlanGenerateResponse,
): GroceryListResponse | null {
  const candidate = response.grocery_list ?? response.household_grocery_list;
  if (isRecord(candidate)) {
    return candidate as GroceryListResponse;
  }
  return null;
}

function memberKey(member: DemoMemberProfile): string {
  return String(member.member_profile_id ?? member.member_id ?? member.display_name ?? "member");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function renderWarnings(warnings: unknown[] | undefined) {
  if (!warnings?.length) {
    return null;
  }
  return (
    <View style={styles.warningBox}>
      <Text style={styles.warningTitle}>Warnings</Text>
      {warnings.slice(0, 4).map((warning, index) => (
        <Text key={`${index}`} style={styles.warningText}>
          {typeof warning === "string" ? warning : JSON.stringify(warning)}
        </Text>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flexGrow: 1,
    padding: 24,
    gap: 20,
  },
  header: {
    gap: 4,
    paddingTop: 18,
  },
  title: {
    color: "#111827",
    fontSize: 34,
    fontWeight: "800",
  },
  subtitle: {
    color: "#4B5563",
    fontSize: 17,
    fontWeight: "600",
  },
  section: {
    gap: 6,
  },
  label: {
    color: "#525252",
    fontSize: 13,
    fontWeight: "700",
    textTransform: "uppercase",
  },
  url: {
    color: "#1F2933",
    fontSize: 16,
    fontWeight: "600",
  },
  panel: {
    gap: 14,
  },
  panelHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },
  panelTitle: {
    color: "#111827",
    fontSize: 18,
    fontWeight: "800",
  },
  panelMeta: {
    color: "#6B7280",
    fontSize: 14,
    fontWeight: "700",
    textAlign: "right",
  },
  memberList: {
    gap: 10,
  },
  householdName: {
    color: "#1F2933",
    fontSize: 16,
    fontWeight: "800",
  },
  dayList: {
    gap: 12,
  },
  button: {
    minHeight: 48,
    alignItems: "center",
    justifyContent: "center",
    borderRadius: 8,
    backgroundColor: "#165D77",
    paddingHorizontal: 16,
  },
  buttonSecondary: {
    borderWidth: 1,
    borderColor: "#165D77",
    backgroundColor: "#FFFFFF",
  },
  buttonPressed: {
    opacity: 0.82,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  buttonText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "800",
  },
  buttonTextSecondary: {
    color: "#165D77",
  },
  infoRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 16,
  },
  infoLabel: {
    color: "#4B5563",
    fontSize: 15,
    fontWeight: "600",
  },
  infoValue: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 15,
    fontWeight: "700",
    textAlign: "right",
  },
  mutedText: {
    color: "#6B7280",
    fontSize: 15,
  },
  errorText: {
    color: "#B42318",
    fontSize: 15,
    fontWeight: "700",
  },
  warningBox: {
    borderWidth: 1,
    borderColor: "#F4C790",
    borderRadius: 8,
    backgroundColor: "#FFF8ED",
    padding: 12,
    gap: 6,
  },
  warningTitle: {
    color: "#7A4B00",
    fontSize: 14,
    fontWeight: "800",
  },
  warningText: {
    color: "#7A4B00",
    fontSize: 13,
    fontWeight: "600",
  },
});
