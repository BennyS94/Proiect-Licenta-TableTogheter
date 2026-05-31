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
import { HouseholdMemberPlanView } from "../components/HouseholdMemberPlanView";
import { HouseholdMemberSwitcher } from "../components/HouseholdMemberSwitcher";
import { MemberCard } from "../components/MemberCard";
import { PlanDayCard } from "../components/PlanDayCard";
import { ProfileCard } from "../components/ProfileCard";
import { ProfileForm } from "../components/ProfileForm";
import { StatusCard } from "../components/StatusCard";
import { API_BASE_URL } from "../config/api";
import {
  createProfile,
  generateHouseholdPlan,
  generateIndividualPlan,
  getDemoHousehold,
  getFeedbackContext,
  getHealth,
  getProfiles,
  submitFeedback,
} from "../services/apiClient";
import type {
  DemoHouseholdResponse,
  DemoMemberProfile,
  FeedbackContextResponse,
  FeedbackType,
  GeneratedMeal,
  GroceryListResponse,
  HealthResponse,
  HouseholdPlanGenerateRequest,
  HouseholdPlanGenerateResponse,
  IndividualPlanGenerateRequest,
  IndividualPlanGenerateResponse,
  MemberProfileCreateRequest,
  MemberProfileResponse,
} from "../types/api";

type HealthState = "idle" | "loading" | "connected" | "error";
type GenerationMode = "individual" | "household";

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
  feedback_enabled: true,
};

const FEEDBACK_TYPES: FeedbackType[] = [
  "liked",
  "disliked",
  "too_long",
  "explicit_avoid",
];

const DEFAULT_HOUSEHOLD_ID = "household_demo_family_001";

export function HomeScreen() {
  const [healthStatus, setHealthStatus] = useState<HealthState>("idle");
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [demoHousehold, setDemoHousehold] = useState<DemoHouseholdResponse | null>(null);
  const [savedProfiles, setSavedProfiles] = useState<MemberProfileResponse[]>([]);
  const [selectedMemberId, setSelectedMemberId] = useState("");
  const [selectedSavedProfileId, setSelectedSavedProfileId] = useState("");
  const [generationMode, setGenerationMode] = useState<GenerationMode>("individual");
  const [selectedHouseholdMemberIds, setSelectedHouseholdMemberIds] = useState<string[]>([]);
  const [currentHouseholdMemberIndex, setCurrentHouseholdMemberIndex] = useState(0);
  const [selectedHouseholdDayIndex, setSelectedHouseholdDayIndex] = useState(1);
  const [isLoadingHousehold, setIsLoadingHousehold] = useState(false);
  const [isLoadingProfiles, setIsLoadingProfiles] = useState(false);
  const [isCreatingProfile, setIsCreatingProfile] = useState(false);
  const [isGeneratingPlan, setIsGeneratingPlan] = useState(false);
  const [isGeneratingHouseholdPlan, setIsGeneratingHouseholdPlan] = useState(false);
  const [generatedPlan, setGeneratedPlan] = useState<IndividualPlanGenerateResponse | null>(
    null,
  );
  const [generatedHouseholdPlan, setGeneratedHouseholdPlan] =
    useState<HouseholdPlanGenerateResponse | null>(null);
  const [feedbackContext, setFeedbackContext] = useState<FeedbackContextResponse | null>(null);
  const [isLoadingFeedbackContext, setIsLoadingFeedbackContext] = useState(false);
  const [pendingFeedbackKey, setPendingFeedbackKey] = useState("");
  const [feedbackMessage, setFeedbackMessage] = useState("");
  const [feedbackError, setFeedbackError] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [householdErrorMessage, setHouseholdErrorMessage] = useState("");
  const [profileMessage, setProfileMessage] = useState("");
  const [profileErrorMessage, setProfileErrorMessage] = useState("");

  const selectedMember = useMemo(
    () =>
      (demoHousehold?.members ?? []).find(
        (member) => memberKey(member) === selectedMemberId,
      ) ?? null,
    [demoHousehold, selectedMemberId],
  );
  const selectedSavedProfile = useMemo(
    () =>
      savedProfiles.find(
        (profile) => profile.member_profile_id === selectedSavedProfileId,
      ) ?? null,
    [savedProfiles, selectedSavedProfileId],
  );
  const selectedHouseholdMembers = useMemo(
    () =>
      (demoHousehold?.members ?? []).filter((member) =>
        selectedHouseholdMemberIds.includes(memberKey(member)),
      ),
    [demoHousehold, selectedHouseholdMemberIds],
  );
  const currentHouseholdMember =
    selectedHouseholdMembers[currentHouseholdMemberIndex] ??
    selectedHouseholdMembers[0] ??
    null;
  const groceryList = generatedPlan ? getGroceryListFromPlanResponse(generatedPlan) : null;
  const householdGroceryList = generatedHouseholdPlan
    ? getHouseholdGroceryListFromResponse(generatedHouseholdPlan)
    : null;
  const savedProfileHouseholdId = demoHousehold?.household_id ?? DEFAULT_HOUSEHOLD_ID;
  const activeHouseholdId = getActiveHouseholdId(
    demoHousehold,
    selectedMember,
    selectedSavedProfile,
  );
  const activeMemberProfileId = selectedSavedProfile
    ? selectedSavedProfile.member_profile_id
    : selectedMember
      ? getMemberProfileId(selectedMember)
      : "";
  const selectedIndividualSource = selectedSavedProfile
    ? "Saved profile"
    : selectedMember
      ? "Demo member"
      : "None";
  const feedbackStats = getFeedbackStats(feedbackContext);

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
    setHouseholdErrorMessage("");
    setGeneratedPlan(null);
    setGeneratedHouseholdPlan(null);
    setFeedbackContext(null);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      const household = await getDemoHousehold();
      const householdMemberIds = household.members.map(memberKey).filter(Boolean);
      setDemoHousehold(household);
      setSelectedMemberId("");
      setSelectedHouseholdMemberIds(householdMemberIds);
      setCurrentHouseholdMemberIndex(0);
      setSelectedHouseholdDayIndex(1);
    } catch (error) {
      setDemoHousehold(null);
      setSelectedMemberId("");
      setSelectedHouseholdMemberIds([]);
      setErrorMessage(error instanceof Error ? error.message : "Household fetch failed");
    } finally {
      setIsLoadingHousehold(false);
    }
  }

  async function loadSavedProfiles(profileIdToSelect?: string, quiet = false) {
    setIsLoadingProfiles(true);
    if (!quiet) {
      setProfileMessage("");
      setProfileErrorMessage("");
    }

    try {
      const profiles = await getProfiles(savedProfileHouseholdId);
      setSavedProfiles(profiles);
      if (profileIdToSelect) {
        setSelectedSavedProfileId(profileIdToSelect);
        setSelectedMemberId("");
      } else if (
        selectedSavedProfileId &&
        !profiles.some((profile) => profile.member_profile_id === selectedSavedProfileId)
      ) {
        setSelectedSavedProfileId("");
      }
    } catch (error) {
      setProfileErrorMessage(error instanceof Error ? error.message : "Profiles fetch failed");
    } finally {
      setIsLoadingProfiles(false);
    }
  }

  async function createSavedProfile(request: MemberProfileCreateRequest) {
    setIsCreatingProfile(true);
    setProfileMessage("");
    setProfileErrorMessage("");

    try {
      const createdProfile = await createProfile(request);
      const profiles = await getProfiles(createdProfile.household_id);
      setSavedProfiles(profiles);
      setSelectedSavedProfileId(createdProfile.member_profile_id);
      setSelectedMemberId("");
      setProfileMessage(`Profile saved: ${createdProfile.display_name}`);
      setGeneratedPlan(null);
      setFeedbackContext(null);
    } catch (error) {
      setProfileErrorMessage(error instanceof Error ? error.message : "Profile save failed");
    } finally {
      setIsCreatingProfile(false);
    }
  }

  async function generatePlanForSelectedMember() {
    if (!selectedMember && !selectedSavedProfile) {
      setErrorMessage("Select a demo member or a saved profile before generating a plan.");
      return;
    }

    setIsGeneratingPlan(true);
    setErrorMessage("");
    setGeneratedPlan(null);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      const request = selectedSavedProfile
        ? buildSavedProfileGenerateRequest(selectedSavedProfile)
        : buildGenerateRequest(selectedMember as DemoMemberProfile, demoHousehold);
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

  async function generateHouseholdPlanForSelectedMembers() {
    if (!demoHousehold) {
      setHouseholdErrorMessage("Load a demo household before generating a household plan.");
      return;
    }
    if (!selectedHouseholdMemberIds.length) {
      setHouseholdErrorMessage("Select at least one member for household generation.");
      return;
    }

    setIsGeneratingHouseholdPlan(true);
    setHouseholdErrorMessage("");
    setGeneratedHouseholdPlan(null);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      const request = buildHouseholdGenerateRequest(demoHousehold, selectedHouseholdMemberIds);
      const response = await generateHouseholdPlan(request);
      setGeneratedHouseholdPlan(response);
      setCurrentHouseholdMemberIndex(0);
      setSelectedHouseholdDayIndex(1);
      if (response.status === "blocked") {
        setHouseholdErrorMessage("Household generation was blocked by the backend.");
      }
    } catch (error) {
      setHouseholdErrorMessage(
        error instanceof Error ? error.message : "Household generate failed",
      );
    } finally {
      setIsGeneratingHouseholdPlan(false);
    }
  }

  function selectDemoMember(memberId: string) {
    setSelectedMemberId(memberId);
    setSelectedSavedProfileId("");
    setFeedbackContext(null);
    setGeneratedPlan(null);
  }

  function selectSavedProfile(memberProfileId: string) {
    setSelectedSavedProfileId(memberProfileId);
    setSelectedMemberId("");
    setFeedbackContext(null);
    setGeneratedPlan(null);
  }

  function toggleHouseholdMember(memberId: string) {
    setHouseholdErrorMessage("");
    setCurrentHouseholdMemberIndex(0);
    setSelectedHouseholdMemberIds((current) => {
      const next = current.includes(memberId)
        ? current.filter((selectedId) => selectedId !== memberId)
        : [...current, memberId];
      return next;
    });
  }

  function showPreviousHouseholdMember() {
    if (!selectedHouseholdMembers.length) {
      return;
    }
    setCurrentHouseholdMemberIndex((current) =>
      current <= 0 ? selectedHouseholdMembers.length - 1 : current - 1,
    );
  }

  function showNextHouseholdMember() {
    if (!selectedHouseholdMembers.length) {
      return;
    }
    setCurrentHouseholdMemberIndex((current) =>
      current >= selectedHouseholdMembers.length - 1 ? 0 : current + 1,
    );
  }

  async function refreshFeedbackContext(quiet = false) {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before refreshing feedback context.");
      return;
    }

    setIsLoadingFeedbackContext(true);
    if (!quiet) {
      setFeedbackMessage("");
      setFeedbackError("");
    }

    try {
      const context = await getFeedbackContext(activeHouseholdId, activeMemberProfileId);
      setFeedbackContext(context);
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback context failed");
    } finally {
      setIsLoadingFeedbackContext(false);
    }
  }

  async function submitMealFeedback(meal: GeneratedMeal, feedbackType: FeedbackType) {
    const recipeId = getMealRecipeId(meal);
    if (!recipeId) {
      setFeedbackError("Feedback unavailable for this meal.");
      return;
    }
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before saving feedback.");
      return;
    }

    const slot = getMealSlot(meal);
    const feedbackKey = buildFeedbackKey(meal, feedbackType);
    setPendingFeedbackKey(feedbackKey);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      await submitFeedback({
        household_id: activeHouseholdId,
        member_profile_id: activeMemberProfileId || undefined,
        plan_id: generatedPlan ? getPlanIdFromResponse(generatedPlan) : undefined,
        recipe_id: recipeId,
        slot: slot || undefined,
        feedback_type: feedbackType,
        source: "mobile",
      });
      setFeedbackMessage("Feedback saved. Generate again to apply it.");
      await refreshFeedbackContext(true);
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback save failed");
    } finally {
      setPendingFeedbackKey("");
    }
  }

  function getPendingFeedbackType(meal: GeneratedMeal): FeedbackType | null {
    if (!pendingFeedbackKey) {
      return null;
    }
    for (const feedbackType of FEEDBACK_TYPES) {
      if (pendingFeedbackKey === buildFeedbackKey(meal, feedbackType)) {
        return feedbackType;
      }
    }
    return null;
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
          <Text style={styles.panelTitle}>Generation mode</Text>
          <Text style={styles.panelMeta}>
            {generationMode === "individual" ? "One member" : "Household"}
          </Text>
        </View>
        <View style={styles.modeSelector}>
          <ModeButton
            label="Individual plan"
            selected={generationMode === "individual"}
            onPress={() => setGenerationMode("individual")}
          />
          <ModeButton
            label="Household plan"
            selected={generationMode === "household"}
            onPress={() => setGenerationMode("household")}
          />
        </View>
      </View>

      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Demo household members</Text>
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
                  selected={
                    generationMode === "individual"
                      ? key === selectedMemberId
                      : selectedHouseholdMemberIds.includes(key)
                  }
                  onPress={() =>
                    generationMode === "individual"
                      ? selectDemoMember(key)
                      : toggleHouseholdMember(key)
                  }
                />
              );
            })}
            {generationMode === "household" ? (
              <Text style={styles.mutedText}>
                Household members selected: {selectedHouseholdMemberIds.length}
              </Text>
            ) : null}
          </View>
        ) : null}
      </View>

      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Saved profiles</Text>
          <Text style={styles.panelMeta}>
            {savedProfiles.length ? `${savedProfiles.length} profiles` : "SQLite demo"}
          </Text>
        </View>
        <ActionButton
          disabled={isLoadingProfiles}
          loading={isLoadingProfiles}
          label="Load saved profiles"
          onPress={() => loadSavedProfiles()}
          variant="secondary"
        />
        <Text style={styles.mutedText}>
          Profiles are stored in backend SQLite for household {savedProfileHouseholdId}.
        </Text>
        <View style={styles.memberList}>
          {savedProfiles.length ? (
            savedProfiles.map((profile) => (
              <ProfileCard
                key={profile.member_profile_id}
                onPress={() => selectSavedProfile(profile.member_profile_id)}
                profile={profile}
                selected={profile.member_profile_id === selectedSavedProfileId}
              />
            ))
          ) : (
            <Text style={styles.mutedText}>No saved profiles loaded.</Text>
          )}
        </View>
        <ProfileForm
          defaultHouseholdId={savedProfileHouseholdId}
          disabled={isCreatingProfile}
          onSubmit={createSavedProfile}
        />
      </View>

      {generationMode === "individual" ? (
        <View style={styles.panel}>
          <View style={styles.panelHeader}>
            <Text style={styles.panelTitle}>Individual plan</Text>
            <Text style={styles.panelMeta}>3 days / grocery / feedback</Text>
          </View>
          <ActionButton
            disabled={(!selectedMember && !selectedSavedProfile) || isGeneratingPlan}
            loading={isGeneratingPlan}
            label="Generate plan for selected profile"
            onPress={generatePlanForSelectedMember}
          />
          {selectedMember || selectedSavedProfile ? (
            <>
              <Text style={styles.mutedText}>Selected source: {selectedIndividualSource}</Text>
              <Text style={styles.mutedText}>
                Selected:{" "}
                {selectedSavedProfile
                  ? selectedSavedProfile.display_name
                  : selectedMember?.display_name ?? selectedMember?.profile_name}
              </Text>
            </>
          ) : (
            <Text style={styles.mutedText}>
              Select a demo member or saved profile to enable generation.
            </Text>
          )}
        </View>
      ) : null}

      {generationMode === "household" ? (
        <View style={styles.panel}>
          <View style={styles.panelHeader}>
            <Text style={styles.panelTitle}>Household plan</Text>
            <Text style={styles.panelMeta}>3 days / shared meals</Text>
          </View>
          <ActionButton
            disabled={!selectedHouseholdMemberIds.length || isGeneratingHouseholdPlan}
            loading={isGeneratingHouseholdPlan}
            label="Generate household plan"
            onPress={generateHouseholdPlanForSelectedMembers}
          />
          {selectedHouseholdMemberIds.length === 1 ? (
            <Text style={styles.mutedText}>
              One member selected. Household endpoint will still be used.
            </Text>
          ) : (
            <Text style={styles.mutedText}>
              Selected members: {selectedHouseholdMemberIds.length}
            </Text>
          )}
          {!selectedHouseholdMemberIds.length ? (
            <Text style={styles.errorText}>Select at least one member.</Text>
          ) : null}
        </View>
      ) : null}

      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Feedback context</Text>
          <Text style={styles.panelMeta}>{feedbackStats.eventCount} events</Text>
        </View>
        <ActionButton
          disabled={!activeHouseholdId || isLoadingFeedbackContext}
          loading={isLoadingFeedbackContext}
          label="Refresh feedback context"
          onPress={() => refreshFeedbackContext(false)}
          variant="secondary"
        />
        <View style={styles.statsGrid}>
          <InfoRow label="Avoided" value={String(feedbackStats.avoidedCount)} />
          <InfoRow label="Liked" value={String(feedbackStats.likedCount)} />
          <InfoRow label="Disliked" value={String(feedbackStats.dislikedCount)} />
          <InfoRow label="Too long" value={String(feedbackStats.tooLongCount)} />
        </View>
        <Text style={styles.mutedText}>
          Next generation uses saved feedback when available.
        </Text>
      </View>

      {errorMessage ? <Text style={styles.errorText}>{errorMessage}</Text> : null}
      {householdErrorMessage ? (
        <Text style={styles.errorText}>{householdErrorMessage}</Text>
      ) : null}
      {profileMessage ? <Text style={styles.successText}>{profileMessage}</Text> : null}
      {profileErrorMessage ? (
        <Text style={styles.errorText}>{profileErrorMessage}</Text>
      ) : null}
      {feedbackMessage ? <Text style={styles.successText}>{feedbackMessage}</Text> : null}
      {feedbackError ? <Text style={styles.errorText}>{feedbackError}</Text> : null}

      {generationMode === "individual" && generatedPlan ? (
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
              <PlanDayCard
                feedbackDisabled={Boolean(pendingFeedbackKey)}
                getPendingFeedbackType={getPendingFeedbackType}
                key={`${day.day_index ?? index}`}
                day={day}
                onSubmitFeedback={submitMealFeedback}
              />
            ))}
          </View>
        </View>
      ) : null}

      {generationMode === "individual" && generatedPlan ? (
        <GroceryListSection groceryList={groceryList} />
      ) : null}

      {generationMode === "household" && generatedHouseholdPlan ? (
        <View style={styles.panel}>
          <View style={styles.panelHeader}>
            <Text style={styles.panelTitle}>Generated household plan</Text>
            <Text style={styles.panelMeta}>Status: {generatedHouseholdPlan.status}</Text>
          </View>
          <InfoRow label="Plan ID" value={getHouseholdPlanId(generatedHouseholdPlan) ?? "missing"} />
          <InfoRow label="Selected members" value={String(selectedHouseholdMembers.length)} />
          <InfoRow label="Days" value={String(generatedHouseholdPlan.days ?? "-")} />
          <InfoRow label="Quality" value={getHouseholdQualitySummary(generatedHouseholdPlan).quality} />
          <InfoRow
            label="Accept/review/reject"
            value={getHouseholdQualitySummary(generatedHouseholdPlan).counts}
          />
          {renderWarnings(generatedHouseholdPlan.warnings)}

          <HouseholdMemberSwitcher
            currentIndex={currentHouseholdMemberIndex}
            members={selectedHouseholdMembers}
            onNext={showNextHouseholdMember}
            onPrevious={showPreviousHouseholdMember}
          />

          {currentHouseholdMember ? (
            <HouseholdMemberPlanView
              memberId={memberKey(currentHouseholdMember)}
              members={selectedHouseholdMembers}
              onSelectDay={setSelectedHouseholdDayIndex}
              plan={generatedHouseholdPlan}
              selectedDayIndex={selectedHouseholdDayIndex}
            />
          ) : (
            <Text style={styles.mutedText}>Select a member to inspect household meals.</Text>
          )}
        </View>
      ) : null}

      {generationMode === "household" && generatedHouseholdPlan ? (
        <GroceryListSection
          emptyMessage="No household grocery list returned."
          groceryList={householdGroceryList}
          title="Household grocery list"
        />
      ) : null}
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

function ModeButton({
  label,
  onPress,
  selected,
}: {
  label: string;
  onPress: () => void;
  selected: boolean;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [
        styles.modeButton,
        selected ? styles.modeButtonSelected : null,
        pressed ? styles.buttonPressed : null,
      ]}
    >
      <Text style={[styles.modeButtonText, selected ? styles.modeButtonTextSelected : null]}>
        {label}
      </Text>
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

function buildGenerateRequest(
  member: DemoMemberProfile,
  household: DemoHouseholdResponse | null,
): IndividualPlanGenerateRequest {
  const profileId = getMemberProfileId(member) || "demo_member";
  const householdId = getActiveHouseholdId(household, member);
  return {
    dataset_profile: "v1_2_demo_final",
    days: 3,
    household_id: householdId || undefined,
    member_profile_id: profileId,
    member_profile: {
      ...member,
      member_profile_id: profileId,
      household_id: householdId || member.household_id,
      profile_name: member.profile_name ?? member.display_name ?? profileId,
    },
    generation_options: GENERATION_OPTIONS,
    include_grocery_list: true,
    include_purchase_suggestions: true,
    include_price_estimates: true,
    feedback_enabled: true,
  };
}

function buildSavedProfileGenerateRequest(
  profile: MemberProfileResponse,
): IndividualPlanGenerateRequest {
  return {
    dataset_profile: "v1_2_demo_final",
    days: 3,
    household_id: profile.household_id,
    member_profile_id: profile.member_profile_id,
    generation_options: GENERATION_OPTIONS,
    include_grocery_list: true,
    include_purchase_suggestions: true,
    include_price_estimates: true,
    feedback_enabled: true,
  };
}

function buildHouseholdGenerateRequest(
  household: DemoHouseholdResponse,
  selectedMemberIds: string[],
): HouseholdPlanGenerateRequest {
  const householdId = String(household.household_id ?? "").trim();
  const members = household.members.map((member) => {
    const profileId = getMemberProfileId(member) || memberKey(member);
    return {
      ...member,
      household_id: householdId || member.household_id,
      member_profile_id: profileId,
      member_id: member.member_id ?? profileId,
      profile_name: member.profile_name ?? member.display_name ?? profileId,
    };
  });

  return {
    dataset_profile: "v1_2_demo_final",
    days: 3,
    household_id: householdId || undefined,
    household_profile: {
      ...household,
      household_id: householdId,
      members,
    },
    selected_member_ids: selectedMemberIds,
    generation_options: GENERATION_OPTIONS,
    include_grocery_list: true,
    include_purchase_suggestions: true,
    include_price_estimates: true,
    feedback_enabled: true,
    household_mode: "individual_breakfast_shared_main",
    household_allocation_mode: "macro_aware_simple",
  };
}

function getActiveHouseholdId(
  household: DemoHouseholdResponse | null,
  member: DemoMemberProfile | null,
  savedProfile?: MemberProfileResponse | null,
): string {
  return String(
    savedProfile?.household_id ?? household?.household_id ?? member?.household_id ?? "",
  ).trim();
}

function getMemberProfileId(member: DemoMemberProfile): string {
  return String(member.member_profile_id ?? member.member_id ?? "").trim();
}

function getPlanIdFromResponse(response: IndividualPlanGenerateResponse): string | undefined {
  const planId = String(response.plan_id ?? response.household_plan_id ?? "").trim();
  return planId || undefined;
}

function getMealRecipeId(meal: GeneratedMeal): string {
  return String(meal.recipe_id ?? "").trim();
}

function getMealSlot(meal: GeneratedMeal): string {
  return String(meal.slot ?? "").trim();
}

function buildFeedbackKey(meal: GeneratedMeal, feedbackType: FeedbackType): string {
  return `${getMealRecipeId(meal)}:${getMealSlot(meal)}:${feedbackType}`;
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

function getHouseholdGroceryListFromResponse(
  response: HouseholdPlanGenerateResponse,
): GroceryListResponse | null {
  const candidate = response.household_grocery_list ?? response.grocery_list;
  if (isRecord(candidate)) {
    return candidate as GroceryListResponse;
  }
  return null;
}

function getHouseholdPlanId(response: HouseholdPlanGenerateResponse): string | undefined {
  const planId = String(response.household_plan_id ?? response.plan_id ?? "").trim();
  return planId || undefined;
}

function getHouseholdQualitySummary(response: HouseholdPlanGenerateResponse) {
  const diagnostics = asRecord(response.diagnostics_summary);
  const quality =
    stringValue(diagnostics.quality) ??
    stringValue(diagnostics.quality_status) ??
    firstDailyAllocationText(response, "quality") ??
    firstDailyAllocationText(response, "status") ??
    "-";
  const acceptCount =
    numberValue(diagnostics.accept_count) ?? sumDailyAllocationCount(response, "accept_count");
  const reviewCount =
    numberValue(diagnostics.review_count) ?? sumDailyAllocationCount(response, "review_count");
  const rejectCount =
    numberValue(diagnostics.reject_count) ?? sumDailyAllocationCount(response, "reject_count");

  return {
    quality,
    counts: `${formatNullableCount(acceptCount)}/${formatNullableCount(
      reviewCount,
    )}/${formatNullableCount(rejectCount)}`,
  };
}

function firstDailyAllocationText(
  response: HouseholdPlanGenerateResponse,
  key: string,
): string | null {
  for (const day of response.daily_plan ?? []) {
    const allocation = asRecord(day.allocation);
    const value = stringValue(allocation[key]);
    if (value) {
      return value;
    }
  }
  return null;
}

function sumDailyAllocationCount(
  response: HouseholdPlanGenerateResponse,
  key: string,
): number | null {
  let total = 0;
  let hasValue = false;
  for (const day of response.daily_plan ?? []) {
    const allocation = asRecord(day.allocation);
    const value = numberValue(allocation[key]);
    if (value !== null) {
      total += value;
      hasValue = true;
    }
  }
  return hasValue ? total : null;
}

function formatNullableCount(value: number | null): string {
  return value === null ? "-" : String(value);
}

function getFeedbackStats(context: FeedbackContextResponse | null) {
  const hardFilters = context?.hard_filters ?? {};
  const scorePreferences = context?.score_preferences ?? {};
  const timePreferences = context?.time_preferences ?? {};

  return {
    eventCount: Number(context?.event_count ?? 0),
    avoidedCount: countBucket(hardFilters.banned_recipe_ids),
    likedCount: countBucket(scorePreferences.liked_recipe_ids),
    dislikedCount: countBucket(scorePreferences.disliked_recipe_ids),
    tooLongCount: countBucket(timePreferences.too_long_recipe_ids),
  };
}

function countBucket(value: unknown): number {
  if (Array.isArray(value)) {
    return value.length;
  }
  if (isRecord(value)) {
    return Object.values(value).reduce<number>((total, item) => {
      if (typeof item === "number" && Number.isFinite(item)) {
        return total + item;
      }
      return total + 1;
    }, 0);
  }
  return 0;
}

function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function stringValue(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function numberValue(value: unknown): number | null {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return null;
  }
  return value;
}

function memberKey(member: DemoMemberProfile): string {
  return String(member.member_profile_id ?? member.member_id ?? member.display_name ?? "member");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function renderWarnings(warnings: unknown[] | string | undefined) {
  const normalizedWarnings = normalizeWarnings(warnings);
  if (!normalizedWarnings.length) {
    return null;
  }
  return (
    <View style={styles.warningBox}>
      <Text style={styles.warningTitle}>Warnings</Text>
      {normalizedWarnings.slice(0, 4).map((warning, index) => (
        <Text key={`${index}`} style={styles.warningText}>
          {warning}
        </Text>
      ))}
    </View>
  );
}

function normalizeWarnings(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  if (value == null) {
    return [];
  }
  return [String(value)];
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
  modeButton: {
    alignItems: "center",
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 12,
  },
  modeButtonSelected: {
    backgroundColor: "#165D77",
  },
  modeButtonText: {
    color: "#165D77",
    fontSize: 14,
    fontWeight: "800",
    textAlign: "center",
  },
  modeButtonTextSelected: {
    color: "#FFFFFF",
  },
  modeSelector: {
    flexDirection: "row",
    gap: 10,
  },
  dayList: {
    gap: 12,
  },
  statsGrid: {
    gap: 8,
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
  successText: {
    color: "#1E7A4C",
    fontSize: 15,
    fontWeight: "800",
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
