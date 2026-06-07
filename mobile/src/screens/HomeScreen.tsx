import { useMemo, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";

import { FloatingNav, type AppPageKey } from "../components/navigation/FloatingNav";
import { GroceryListSection } from "../components/GroceryListSection";
import { HouseholdMemberPlanView } from "../components/HouseholdMemberPlanView";
import { HouseholdMemberSwitcher } from "../components/HouseholdMemberSwitcher";
import { MemberCard } from "../components/MemberCard";
import { PlanDayCard } from "../components/PlanDayCard";
import { ProfileCard } from "../components/ProfileCard";
import { ProfileForm } from "../components/ProfileForm";
import { AppCard } from "../components/ui/AppCard";
import { DaySelector } from "../components/ui/DaySelector";
import { ProfileSelector, type ProfileSelectorItem } from "../components/ui/ProfileSelector";
import { API_BASE_URL } from "../config/api";
import { HomePage } from "./HomePage";
import { HouseholdPage } from "./HouseholdPage";
import {
  InsightsPage,
  type InsightsDaySelection,
  type InsightsTotals,
  type MealContribution,
} from "./InsightsPage";
import { MealPlanPage, type MealPlanTab } from "./MealPlanPage";
import {
  clearFeedback,
  createProfile,
  deleteProfile,
  generateHouseholdPlan,
  generateIndividualPlan,
  getDemoHousehold,
  getFeedbackContext,
  getHealth,
  getProfiles,
  loginAccount,
  logoutAccount,
  registerAccount,
  submitFeedback,
} from "../services/apiClient";
import type {
  AuthAccount,
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
  MealReplacementResponse,
  MemberProfileCreateRequest,
  MemberProfileResponse,
} from "../types/api";

type HealthState = "idle" | "loading" | "connected" | "error";
type GenerationMode = "individual" | "household";
type HouseholdSource = "demo" | "saved";

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
  const [householdSource, setHouseholdSource] = useState<HouseholdSource>("demo");
  const [selectedHouseholdMemberIds, setSelectedHouseholdMemberIds] = useState<string[]>([]);
  const [selectedSavedHouseholdProfileIds, setSelectedSavedHouseholdProfileIds] = useState<
    string[]
  >([]);
  const [currentHouseholdMemberIndex, setCurrentHouseholdMemberIndex] = useState(0);
  const [selectedHouseholdDayIndex, setSelectedHouseholdDayIndex] = useState(1);
  const [isLoadingHousehold, setIsLoadingHousehold] = useState(false);
  const [isLoadingProfiles, setIsLoadingProfiles] = useState(false);
  const [isCreatingProfile, setIsCreatingProfile] = useState(false);
  const [deletingProfileId, setDeletingProfileId] = useState("");
  const [isGeneratingPlan, setIsGeneratingPlan] = useState(false);
  const [isGeneratingHouseholdPlan, setIsGeneratingHouseholdPlan] = useState(false);
  const [generatedPlan, setGeneratedPlan] = useState<IndividualPlanGenerateResponse | null>(
    null,
  );
  const [generatedHouseholdPlan, setGeneratedHouseholdPlan] =
    useState<HouseholdPlanGenerateResponse | null>(null);
  const [feedbackContext, setFeedbackContext] = useState<FeedbackContextResponse | null>(null);
  const [isLoadingFeedbackContext, setIsLoadingFeedbackContext] = useState(false);
  const [isClearingFeedback, setIsClearingFeedback] = useState(false);
  const [pendingFeedbackKey, setPendingFeedbackKey] = useState("");
  const [feedbackMessage, setFeedbackMessage] = useState("");
  const [feedbackError, setFeedbackError] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [householdErrorMessage, setHouseholdErrorMessage] = useState("");
  const [profileMessage, setProfileMessage] = useState("");
  const [profileErrorMessage, setProfileErrorMessage] = useState("");
  const [authMessage, setAuthMessage] = useState("");
  const [authError, setAuthError] = useState("");
  const [authSessionToken, setAuthSessionToken] = useState("");
  const [authAccount, setAuthAccount] = useState<AuthAccount | null>(null);
  const [isAuthLoading, setIsAuthLoading] = useState(false);
  const [activePage, setActivePage] = useState<AppPageKey>("household");
  const [isDemoModeEnabled, setIsDemoModeEnabled] = useState(false);
  const [isContinuingDemo, setIsContinuingDemo] = useState(false);
  const [defaultViewerId, setDefaultViewerId] = useState("");
  const [mealPlanTab, setMealPlanTab] = useState<MealPlanTab>("mealPlan");
  const [selectedIndividualDayIndex, setSelectedIndividualDayIndex] = useState(1);
  const [selectedInsightsDay, setSelectedInsightsDay] =
    useState<InsightsDaySelection>(1);
  const [planDays, setPlanDays] = useState(3);

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
  const selectedSavedHouseholdProfiles = useMemo(
    () =>
      savedProfiles.filter((profile) =>
        selectedSavedHouseholdProfileIds.includes(profile.member_profile_id),
      ),
    [savedProfiles, selectedSavedHouseholdProfileIds],
  );
  const selectedHouseholdDisplayMembers = useMemo(
    () =>
      getHouseholdDisplayMembers(
        householdSource,
        selectedHouseholdMembers,
        selectedSavedHouseholdProfiles,
        generatedHouseholdPlan,
      ),
    [
      generatedHouseholdPlan,
      householdSource,
      selectedHouseholdMembers,
      selectedSavedHouseholdProfiles,
    ],
  );
  const currentHouseholdMember =
    selectedHouseholdDisplayMembers[currentHouseholdMemberIndex] ??
    selectedHouseholdDisplayMembers[0] ??
    null;
  const groceryList = generatedPlan ? getGroceryListFromPlanResponse(generatedPlan) : null;
  const householdGroceryList = generatedHouseholdPlan
    ? getHouseholdGroceryListFromResponse(generatedHouseholdPlan)
    : null;
  const savedProfileHouseholdId =
    authAccount?.household_id ?? demoHousehold?.household_id ?? DEFAULT_HOUSEHOLD_ID;
  const selectedHouseholdCount =
    householdSource === "saved"
      ? selectedSavedHouseholdProfiles.length
      : selectedHouseholdMemberIds.length;
  const selectedSavedHouseholdId = getSelectedSavedProfilesHouseholdId(
    selectedSavedHouseholdProfiles,
  );
  const savedProfileHouseholdMismatch = hasMixedHouseholdIds(selectedSavedHouseholdProfiles);
  const selectedSavedHouseholdNames = selectedSavedHouseholdProfiles
    .map((profile) => profile.display_name)
    .filter(Boolean)
    .join(", ");
  const householdGenerateDisabled =
    isGeneratingHouseholdPlan ||
    selectedHouseholdCount < 1 ||
    (householdSource === "saved" && savedProfileHouseholdMismatch);
  const activeHouseholdId = getActiveHouseholdId(
    demoHousehold,
    selectedMember,
    selectedSavedProfile,
    generationMode === "household" && householdSource === "saved"
      ? selectedSavedHouseholdId || savedProfileHouseholdId
      : "",
  );
  const activeMemberProfileId = selectedSavedProfile
    ? selectedSavedProfile.member_profile_id
    : selectedMember
      ? getMemberProfileId(selectedMember)
      : "";
  const selectedIndividualSource = selectedSavedProfile
    ? "Saved profile"
    : selectedMember
      ? "Sample member"
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
      const firstMemberId = householdMemberIds[0] ?? "";
      setDemoHousehold(household);
      setSelectedMemberId(firstMemberId);
      setDefaultViewerId(firstMemberId ? `demo:${firstMemberId}` : "");
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
      const profiles = await getProfiles(savedProfileHouseholdId, authSessionToken);
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
      setSelectedSavedHouseholdProfileIds((current) =>
        current.filter((profileId) =>
          profiles.some((profile) => profile.member_profile_id === profileId),
        ),
      );
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
      const createdProfile = await createProfile(request, authSessionToken);
      const profiles = await getProfiles(createdProfile.household_id, authSessionToken);
      setSavedProfiles(profiles);
      setSelectedSavedProfileId(createdProfile.member_profile_id);
      setDefaultViewerId(`saved:${createdProfile.member_profile_id}`);
      setSelectedMemberId("");
      setHouseholdSource("saved");
      setSelectedSavedHouseholdProfileIds((current) =>
        current.includes(createdProfile.member_profile_id)
          ? current
          : [...current, createdProfile.member_profile_id],
      );
      setProfileMessage(`Profile saved: ${createdProfile.display_name}`);
      setGeneratedPlan(null);
      setFeedbackContext(null);
      if (!savedProfiles.length) {
        setActivePage("mealPlan");
      }
    } catch (error) {
      setProfileErrorMessage(error instanceof Error ? error.message : "Profile save failed");
    } finally {
      setIsCreatingProfile(false);
    }
  }

  function confirmDeleteSavedProfile(profile: MemberProfileResponse) {
    Alert.alert(
      "Remove this saved profile?",
      `${profile.display_name} will be deactivated in local SQLite.`,
      [
        {
          text: "Cancel",
          style: "cancel",
        },
        {
          text: "Remove",
          style: "destructive",
          onPress: () => {
            void deleteSavedProfile(profile);
          },
        },
      ],
    );
  }

  async function deleteSavedProfile(profile: MemberProfileResponse) {
    const memberProfileId = profile.member_profile_id;
    const wasSelectedProfile = selectedSavedProfileId === memberProfileId;
    const wasSelectedForHousehold =
      selectedSavedHouseholdProfileIds.includes(memberProfileId);

    setDeletingProfileId(memberProfileId);
    setProfileMessage("");
    setProfileErrorMessage("");

    try {
      await deleteProfile(memberProfileId, authSessionToken);
      const profiles = await getProfiles(
        profile.household_id || savedProfileHouseholdId,
        authSessionToken,
      );
      setSavedProfiles(profiles);
      setSelectedSavedProfileId((current) =>
        current === memberProfileId ? "" : current,
      );
      setSelectedSavedHouseholdProfileIds((current) =>
        current.filter((profileId) => profileId !== memberProfileId),
      );
      if (wasSelectedProfile) {
        setGeneratedPlan(null);
        setFeedbackContext(null);
      }
      if (wasSelectedForHousehold) {
        setGeneratedHouseholdPlan(null);
        setCurrentHouseholdMemberIndex(0);
        setSelectedHouseholdDayIndex(1);
      }
      setProfileMessage(`Profile removed: ${profile.display_name}`);
    } catch (error) {
      setProfileErrorMessage(error instanceof Error ? error.message : "Profile remove failed");
    } finally {
      setDeletingProfileId("");
    }
  }

  async function generatePlanForSelectedMember() {
    if (!selectedMember && !selectedSavedProfile) {
      setErrorMessage("Select or create a profile before generating a plan.");
      return;
    }

    setIsGeneratingPlan(true);
    setErrorMessage("");
    setGeneratedPlan(null);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      const request = selectedSavedProfile
        ? buildSavedProfileGenerateRequest(selectedSavedProfile, planDays)
        : buildGenerateRequest(selectedMember as DemoMemberProfile, demoHousehold, planDays);
      const response = await generateIndividualPlan(request);
      setGeneratedPlan(response);
      setSelectedIndividualDayIndex(1);
      setSelectedInsightsDay(1);
      setMealPlanTab("mealPlan");
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
    if (householdSource === "demo" && !demoHousehold) {
      setHouseholdErrorMessage("Open a sample household before generating this plan.");
      return;
    }
    if (selectedHouseholdCount < 1) {
      setHouseholdErrorMessage("Select at least one member for household generation.");
      return;
    }
    if (householdSource === "saved" && savedProfileHouseholdMismatch) {
      setHouseholdErrorMessage("Select saved profiles from one household.");
      return;
    }

    setIsGeneratingHouseholdPlan(true);
    setHouseholdErrorMessage("");
    setGeneratedHouseholdPlan(null);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      const request =
        householdSource === "saved"
          ? buildSavedProfilesHouseholdGenerateRequest(selectedSavedHouseholdProfiles, planDays)
          : buildHouseholdGenerateRequest(
              demoHousehold as DemoHouseholdResponse,
              selectedHouseholdMemberIds,
              planDays,
            );
      const response = await generateHouseholdPlan(request);
      setGeneratedHouseholdPlan(response);
      setCurrentHouseholdMemberIndex(0);
      setSelectedHouseholdDayIndex(1);
      setSelectedInsightsDay(1);
      setMealPlanTab("mealPlan");
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
    setDefaultViewerId(`demo:${memberId}`);
    setFeedbackContext(null);
    setGeneratedPlan(null);
  }

  function selectSavedProfile(memberProfileId: string) {
    setSelectedSavedProfileId(memberProfileId);
    setSelectedMemberId("");
    setDefaultViewerId(`saved:${memberProfileId}`);
    setFeedbackContext(null);
    setGeneratedPlan(null);
  }

  function selectHouseholdSource(source: HouseholdSource) {
    setHouseholdSource(source);
    setHouseholdErrorMessage("");
    setGeneratedHouseholdPlan(null);
    setCurrentHouseholdMemberIndex(0);
    setSelectedHouseholdDayIndex(1);
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

  function toggleSavedHouseholdProfile(memberProfileId: string) {
    setHouseholdErrorMessage("");
    setCurrentHouseholdMemberIndex(0);
    setSelectedHouseholdDayIndex(1);
    setSelectedSavedHouseholdProfileIds((current) =>
      current.includes(memberProfileId)
        ? current.filter((selectedId) => selectedId !== memberProfileId)
        : [...current, memberProfileId],
    );
  }

  function handleSavedProfilePress(memberProfileId: string) {
    if (generationMode === "household") {
      if (householdSource !== "saved") {
        setHouseholdSource("saved");
        setGeneratedHouseholdPlan(null);
      }
      toggleSavedHouseholdProfile(memberProfileId);
      return;
    }
    selectSavedProfile(memberProfileId);
  }

  function handleDemoMemberPress(memberId: string) {
    if (generationMode === "individual") {
      selectDemoMember(memberId);
      return;
    }
    if (householdSource !== "demo") {
      setHouseholdSource("demo");
      setGeneratedHouseholdPlan(null);
    }
    toggleHouseholdMember(memberId);
  }

  function showPreviousHouseholdMember() {
    if (!selectedHouseholdDisplayMembers.length) {
      return;
    }
    setCurrentHouseholdMemberIndex((current) =>
      current <= 0 ? selectedHouseholdDisplayMembers.length - 1 : current - 1,
    );
  }

  function showNextHouseholdMember() {
    if (!selectedHouseholdDisplayMembers.length) {
      return;
    }
    setCurrentHouseholdMemberIndex((current) =>
      current >= selectedHouseholdDisplayMembers.length - 1 ? 0 : current + 1,
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

  function confirmClearFeedback() {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before clearing feedback.");
      return;
    }

    Alert.alert(
      "Clear feedback?",
      "This removes local feedback events for the active household.",
      [
        {
          text: "Cancel",
          style: "cancel",
        },
        {
          text: "Clear",
          style: "destructive",
          onPress: () => {
            void clearFeedbackForActiveHousehold();
          },
        },
      ],
    );
  }

  async function clearFeedbackForActiveHousehold() {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before clearing feedback.");
      return;
    }

    setIsClearingFeedback(true);
    setFeedbackMessage("");
    setFeedbackError("");

    try {
      const response = await clearFeedback(activeHouseholdId, undefined, true);
      await refreshFeedbackContext(true);
      setFeedbackMessage(`Feedback cleared (${response.deleted_event_count} events).`);
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback clear failed");
    } finally {
      setIsClearingFeedback(false);
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

  function handleMealReplacementApplied(response: MealReplacementResponse) {
    const updatedPlan = response.updated_plan;
    if (!isRecord(updatedPlan)) {
      setFeedbackError("Meal replacement response did not include an updated plan.");
      return;
    }

    if (response.generation_type === "household" || updatedPlan.generation_type === "household") {
      setGeneratedHouseholdPlan(updatedPlan as HouseholdPlanGenerateResponse);
    } else {
      setGeneratedPlan(updatedPlan as IndividualPlanGenerateResponse);
    }
    setFeedbackMessage("Meal replaced. Plan and grocery list updated.");
    setFeedbackError("");
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

  async function continueAsDemo() {
    setIsContinuingDemo(true);
    setErrorMessage("");
    setProfileErrorMessage("");
    setAuthError("");
    setAuthMessage("");

    try {
      setIsDemoModeEnabled(true);
      setAuthSessionToken("");
      setAuthAccount(null);
      setSavedProfiles([]);
      await loadDemoHousehold();
      setActivePage("home");
    } finally {
      setIsContinuingDemo(false);
    }
  }

  async function registerLocalAccount(
    email: string,
    password: string,
    confirmPassword: string,
  ) {
    setIsAuthLoading(true);
    setAuthError("");
    setAuthMessage("");
    setProfileErrorMessage("");

    try {
      const response = await registerAccount(email, password, confirmPassword);
      await applyAuthResponse(response.session_token, response.account, "Account created.");
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : "Account creation failed");
    } finally {
      setIsAuthLoading(false);
    }
  }

  async function loginLocalAccount(email: string, password: string) {
    setIsAuthLoading(true);
    setAuthError("");
    setAuthMessage("");
    setProfileErrorMessage("");

    try {
      const response = await loginAccount(email, password);
      await applyAuthResponse(response.session_token, response.account, "Logged in.");
    } catch (error) {
      setAuthError(error instanceof Error ? error.message : "Login failed");
    } finally {
      setIsAuthLoading(false);
    }
  }

  async function applyAuthResponse(
    sessionToken: string,
    account: AuthAccount,
    message: string,
  ) {
    setAuthSessionToken(sessionToken);
    setAuthAccount(account);
    setAuthMessage(message);
    setIsDemoModeEnabled(false);
    setDemoHousehold(null);
    setSelectedMemberId("");
    setHouseholdSource("saved");
    setSelectedHouseholdMemberIds([]);
    setGeneratedPlan(null);
    setGeneratedHouseholdPlan(null);
    setFeedbackContext(null);
    setFeedbackMessage("");
    setFeedbackError("");
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setProfileMessage("");
    const profiles = await getProfiles(account.household_id, sessionToken);
    setSavedProfiles(profiles);
    const firstProfile = profiles[0];
    if (firstProfile) {
      setSelectedSavedProfileId(firstProfile.member_profile_id);
      setDefaultViewerId(`saved:${firstProfile.member_profile_id}`);
      setSelectedSavedHouseholdProfileIds([firstProfile.member_profile_id]);
    } else {
      setSelectedSavedProfileId("");
      setDefaultViewerId("");
      setSelectedSavedHouseholdProfileIds([]);
    }
    setActivePage("household");
  }

  async function logOutLocalSession() {
    if (authSessionToken) {
      try {
        await logoutAccount(authSessionToken);
      } catch {
        // Logout-ul local trebuie sa continue chiar daca backend-ul nu raspunde.
      }
    }
    setHealthStatus("idle");
    setHealth(null);
    setAuthSessionToken("");
    setAuthAccount(null);
    setAuthMessage("");
    setAuthError("");
    setDemoHousehold(null);
    setSavedProfiles([]);
    setSelectedMemberId("");
    setSelectedSavedProfileId("");
    setGenerationMode("individual");
    setHouseholdSource("demo");
    setSelectedHouseholdMemberIds([]);
    setSelectedSavedHouseholdProfileIds([]);
    setCurrentHouseholdMemberIndex(0);
    setSelectedHouseholdDayIndex(1);
    setGeneratedPlan(null);
    setGeneratedHouseholdPlan(null);
    setFeedbackContext(null);
    setPendingFeedbackKey("");
    setFeedbackMessage("");
    setFeedbackError("");
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setProfileMessage("");
    setProfileErrorMessage("");
    setIsDemoModeEnabled(false);
    setDefaultViewerId("");
    setMealPlanTab("mealPlan");
    setSelectedIndividualDayIndex(1);
    setSelectedInsightsDay(1);
    setActivePage("household");
    Alert.alert("Log Out", "Local session cleared.");
  }

  function showUnavailableAction(label: string) {
    Alert.alert(label, "This account action is not available in the local preview yet.");
  }

  function selectProfileFromSelector(profileId: string) {
    setDefaultViewerId(profileId);
    if (profileId.startsWith("saved:")) {
      selectSavedProfile(profileId.replace("saved:", ""));
      return;
    }
    if (profileId.startsWith("demo:")) {
      selectDemoMember(profileId.replace("demo:", ""));
    }
  }

  function handleInsightsDaySelect(value: InsightsDaySelection) {
    setSelectedInsightsDay(value);
  }

  const isSetupComplete =
    Boolean(authAccount) || isDemoModeEnabled || Boolean(demoHousehold) || savedProfiles.length > 0;
  const configuredProfileCount =
    (isDemoModeEnabled || demoHousehold ? demoHousehold?.members.length ?? 0 : 0) +
    savedProfiles.length;
  const hasMembers = configuredProfileCount > 0;
  const hasCurrentPlan =
    generationMode === "individual" ? Boolean(generatedPlan) : Boolean(generatedHouseholdPlan);
  const profileSelectorItems = buildProfileSelectorItems(demoHousehold, savedProfiles);
  const selectedProfileSelectorId =
    selectedSavedProfile
      ? `saved:${selectedSavedProfile.member_profile_id}`
      : selectedMember
        ? `demo:${memberKey(selectedMember)}`
        : defaultViewerId || profileSelectorItems[0]?.id || "";
  const selectedProfileItem =
    profileSelectorItems.find((item) => item.id === selectedProfileSelectorId) ??
    profileSelectorItems[0] ??
    null;
  const activeProfileName = selectedProfileItem?.label ?? "Your household";
  const activeProfileMeta =
    selectedProfileItem?.meta ?? "Goal: Muscle gain - 3 meals + snack";
  const householdName =
    authAccount?.household_display_name ?? (demoHousehold ? "Sample Household" : "My Household");
  const backendStatusText =
    healthStatus === "connected" ? "Connected" : healthStatus === "loading" ? "Checking" : "Unknown";
  const individualDayIndexes = getIndividualDayIndexes(generatedPlan);
  const selectedIndividualDay =
    getIndividualDay(generatedPlan, selectedIndividualDayIndex) ??
    getIndividualDay(generatedPlan, individualDayIndexes[0] ?? 1);
  const insightsDayIndexes =
    generationMode === "household"
      ? getHouseholdDayIndexes(generatedHouseholdPlan)
      : individualDayIndexes;
  const safeInsightsDay =
    selectedInsightsDay === "average" || insightsDayIndexes.includes(selectedInsightsDay)
      ? selectedInsightsDay
      : insightsDayIndexes[0] ?? 1;
  const householdInsightsMemberId = currentHouseholdMember
    ? memberKey(currentHouseholdMember)
    : "";
  const insightsTotals =
    generationMode === "household"
      ? getHouseholdInsightsTotals(generatedHouseholdPlan, householdInsightsMemberId, safeInsightsDay)
      : getIndividualInsightsTotals(generatedPlan, safeInsightsDay);
  const insightsTargetTotals =
    generationMode === "household"
      ? getHouseholdTargetTotals(generatedHouseholdPlan, householdInsightsMemberId)
      : undefined;
  const insightMealContributions =
    generationMode === "household"
      ? getHouseholdMealContributions(
          generatedHouseholdPlan,
          householdInsightsMemberId,
          safeInsightsDay,
        )
      : getIndividualMealContributions(generatedPlan, safeInsightsDay);

  const profileSelectorNode = profileSelectorItems.length ? (
    <ProfileSelector
      items={profileSelectorItems}
      onSelect={selectProfileFromSelector}
      selectedId={selectedProfileSelectorId}
    />
  ) : null;

  const messagesContent =
    errorMessage ||
    householdErrorMessage ||
    profileMessage ||
    profileErrorMessage ||
    (isSetupComplete && authMessage) ||
    (isSetupComplete && authError) ||
    feedbackMessage ||
    feedbackError ? (
      <View style={styles.messageStack}>
        {errorMessage ? <Text style={styles.errorText}>{errorMessage}</Text> : null}
        {householdErrorMessage ? (
          <Text style={styles.errorText}>{householdErrorMessage}</Text>
        ) : null}
        {profileMessage ? <Text style={styles.successText}>{profileMessage}</Text> : null}
        {profileErrorMessage ? (
          <Text style={styles.errorText}>{profileErrorMessage}</Text>
        ) : null}
        {isSetupComplete && authMessage ? (
          <Text style={styles.successText}>{authMessage}</Text>
        ) : null}
        {isSetupComplete && authError ? <Text style={styles.errorText}>{authError}</Text> : null}
        {feedbackMessage ? <Text style={styles.successText}>{feedbackMessage}</Text> : null}
        {feedbackError ? <Text style={styles.errorText}>{feedbackError}</Text> : null}
      </View>
    ) : null;

  const generationControls = (
    <View style={styles.stack}>
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
      <DayCountSelector days={planDays} onChange={setPlanDays} />
      {generationMode === "individual" ? (
        <>
          <ActionButton
            disabled={(!selectedMember && !selectedSavedProfile) || isGeneratingPlan}
            loading={isGeneratingPlan}
            label={generatedPlan ? "Generate Again" : "Generate Menu"}
            onPress={generatePlanForSelectedMember}
          />
          <Text style={styles.mutedText}>Selected source: {selectedIndividualSource}</Text>
          <Text style={styles.mutedText}>
            Selected:{" "}
            {selectedSavedProfile
              ? selectedSavedProfile.display_name
              : selectedMember?.display_name ?? selectedMember?.profile_name ?? "None"}
          </Text>
        </>
      ) : (
        <>
          <ActionButton
            disabled={householdGenerateDisabled}
            loading={isGeneratingHouseholdPlan}
            label={generatedHouseholdPlan ? "Generate Again" : "Generate Menu"}
            onPress={generateHouseholdPlanForSelectedMembers}
          />
          <Text style={styles.mutedText}>Selected members: {selectedHouseholdCount}</Text>
          {householdSource === "saved" && selectedSavedHouseholdNames ? (
            <Text style={styles.mutedText}>Selected saved profiles: {selectedSavedHouseholdNames}</Text>
          ) : null}
          {savedProfileHouseholdMismatch ? (
            <Text style={styles.errorText}>Select saved profiles from one household.</Text>
          ) : null}
        </>
      )}
    </View>
  );

  const daySelectorNode =
    generationMode === "individual" && generatedPlan ? (
      <DaySelector
        dayIndexes={individualDayIndexes}
        onSelect={(value) => {
          if (typeof value === "number") {
            setSelectedIndividualDayIndex(value);
          }
        }}
        selected={selectedIndividualDayIndex}
      />
    ) : null;

  const individualPlanContent =
    generationMode === "individual" && generatedPlan ? (
      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Generated plan</Text>
          <Text style={styles.panelMeta}>Status: {generatedPlan.status}</Text>
        </View>
        <InfoRow label="Plan ID" value={generatedPlan.plan_id ?? "missing"} />
        <InfoRow label="Days" value={String(generatedPlan.days ?? "-")} />
        {renderWarnings(generatedPlan.warnings)}
        {selectedIndividualDay ? (
          <PlanDayCard
            datasetProfile="v1_2_demo_final"
            day={selectedIndividualDay}
            feedbackDisabled={Boolean(pendingFeedbackKey)}
            getPendingFeedbackType={getPendingFeedbackType}
            householdId={activeHouseholdId || undefined}
            memberProfile={selectedSavedProfile ? undefined : selectedMember ?? undefined}
            memberProfileId={activeMemberProfileId || undefined}
            onReplacementApplied={handleMealReplacementApplied}
            onSubmitFeedback={submitMealFeedback}
            planId={getPlanIdFromResponse(generatedPlan)}
          />
        ) : (
          <Text style={styles.mutedText}>No meals returned for this day.</Text>
        )}
      </View>
    ) : null;

  const householdPlanContent =
    generationMode === "household" && generatedHouseholdPlan ? (
      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Generated household plan</Text>
          <Text style={styles.panelMeta}>Status: {generatedHouseholdPlan.status}</Text>
        </View>
        <InfoRow label="Plan ID" value={getHouseholdPlanId(generatedHouseholdPlan) ?? "missing"} />
        <InfoRow label="Selected members" value={String(selectedHouseholdDisplayMembers.length)} />
        <InfoRow label="Days" value={String(generatedHouseholdPlan.days ?? "-")} />
        <InfoRow label="Quality" value={getHouseholdQualitySummary(generatedHouseholdPlan).quality} />
        <InfoRow
          label="Accept/review/reject"
          value={getHouseholdQualitySummary(generatedHouseholdPlan).counts}
        />
        {renderWarnings(generatedHouseholdPlan.warnings)}
        <HouseholdMemberSwitcher
          currentIndex={currentHouseholdMemberIndex}
          members={selectedHouseholdDisplayMembers}
          onNext={showNextHouseholdMember}
          onPrevious={showPreviousHouseholdMember}
        />
        {currentHouseholdMember ? (
          <HouseholdMemberPlanView
            datasetProfile={generatedHouseholdPlan.dataset_profile ?? "v1_2_demo_final"}
            householdId={generatedHouseholdPlan.household_id || activeHouseholdId || undefined}
            memberId={memberKey(currentHouseholdMember)}
            members={selectedHouseholdDisplayMembers}
            onReplacementApplied={handleMealReplacementApplied}
            onSelectDay={setSelectedHouseholdDayIndex}
            plan={generatedHouseholdPlan}
            planId={getHouseholdPlanId(generatedHouseholdPlan)}
            selectedDayIndex={selectedHouseholdDayIndex}
          />
        ) : (
          <Text style={styles.mutedText}>Select a member to inspect household meals.</Text>
        )}
      </View>
    ) : null;

  const groceryContent =
    generationMode === "household" ? (
      <GroceryListSection
        emptyMessage="No household grocery list returned."
        groceryList={householdGroceryList}
        title="Household grocery list"
      />
    ) : (
      <GroceryListSection groceryList={groceryList} />
    );

  const feedbackToolsContent = (
    <AppCard>
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
      <ActionButton
        disabled={!activeHouseholdId || isClearingFeedback}
        loading={isClearingFeedback}
        label="Clear feedback"
        onPress={confirmClearFeedback}
        variant="secondary"
      />
      <View style={styles.statsGrid}>
        <InfoRow label="Avoided" value={String(feedbackStats.avoidedCount)} />
        <InfoRow label="Liked" value={String(feedbackStats.likedCount)} />
        <InfoRow label="Disliked" value={String(feedbackStats.dislikedCount)} />
        <InfoRow label="Too long" value={String(feedbackStats.tooLongCount)} />
      </View>
      <Text style={styles.mutedText}>Next generation uses saved feedback when available.</Text>
    </AppCard>
  );

  const householdManagementContent = (
    <View style={styles.stack}>
      {isDemoModeEnabled && demoHousehold ? (
        <View style={styles.panel}>
          <View style={styles.panelHeader}>
            <Text style={styles.panelTitle}>Sample household members</Text>
            <Text style={styles.panelMeta}>{demoHousehold.members.length} members</Text>
          </View>
          <View style={styles.memberList}>
            {demoHousehold.members.map((member) => {
              const key = memberKey(member);
              return (
                <MemberCard
                  key={key}
                  member={member}
                  selected={
                    generationMode === "individual"
                      ? key === selectedMemberId
                      : householdSource === "demo" && selectedHouseholdMemberIds.includes(key)
                  }
                  onPress={() => handleDemoMemberPress(key)}
                />
              );
            })}
            {generationMode === "household" && householdSource === "demo" ? (
              <Text style={styles.mutedText}>
                Household members selected: {selectedHouseholdMemberIds.length}
              </Text>
            ) : null}
          </View>
        </View>
      ) : null}

      <View style={styles.panel}>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Member profiles</Text>
          <Text style={styles.panelMeta}>
            {savedProfiles.length ? `${savedProfiles.length} profiles` : "No profiles"}
          </Text>
        </View>
        <ActionButton
          disabled={isLoadingProfiles}
          loading={isLoadingProfiles}
          label="Refresh profiles"
          onPress={() => loadSavedProfiles()}
          variant="secondary"
        />
        <Text style={styles.mutedText}>
          Profiles are saved for {householdName}.
        </Text>
        <View style={styles.memberList}>
          {savedProfiles.length ? (
            savedProfiles.map((profile) => {
              const selected =
                generationMode === "household" && householdSource === "saved"
                  ? selectedSavedHouseholdProfileIds.includes(profile.member_profile_id)
                  : profile.member_profile_id === selectedSavedProfileId;
              return (
                <ProfileCard
                  key={profile.member_profile_id}
                  deleteDisabled={deletingProfileId === profile.member_profile_id}
                  onDelete={() => confirmDeleteSavedProfile(profile)}
                  onPress={() => handleSavedProfilePress(profile.member_profile_id)}
                  profile={profile}
                  selected={selected}
                />
              );
            })
          ) : (
            <Text style={styles.mutedText}>No profiles yet.</Text>
          )}
        </View>
        {generationMode === "household" && householdSource === "saved" ? (
          <Text style={styles.mutedText}>
            Household members selected: {selectedSavedHouseholdProfileIds.length}
          </Text>
        ) : null}
        <ProfileForm
          defaultHouseholdId={savedProfileHouseholdId}
          disabled={isCreatingProfile}
          onSubmit={createSavedProfile}
        />
      </View>
    </View>
  );

  const defaultViewerContent = profileSelectorItems.length ? (
    <View style={styles.stack}>
      <Text style={styles.mutedText}>Who is using this device?</Text>
      {profileSelectorNode}
      <Text style={styles.mutedText}>
        This controls which profile appears first in Meal Plan and Insights. You can still
        switch profiles.
      </Text>
    </View>
  ) : (
    <Text style={styles.mutedText}>Add or load profiles to choose a default viewer.</Text>
  );

  const appSettingsContent = (
    <View style={styles.stack}>
      <View style={styles.section}>
        <Text style={styles.label}>Backend URL</Text>
        <Text style={styles.url}>{API_BASE_URL}</Text>
      </View>
      <ActionButton
        disabled={healthStatus === "loading"}
        loading={healthStatus === "loading"}
        label="Check backend health"
        onPress={checkBackendHealth}
        variant="secondary"
      />
      {healthStatus === "connected" && health ? (
        <View style={styles.statsGrid}>
          <InfoRow label="Service" value={health.service} />
          <InfoRow label="Version" value={health.version} />
          <InfoRow label="Database" value={health.database} />
        </View>
      ) : null}
    </View>
  );

  let pageContent;
  if (activePage === "home") {
    pageContent = (
      <HomePage
        activeProfileName={activeProfileName}
        householdName={householdName}
        isSetupComplete={isSetupComplete}
        onGoToHousehold={() => setActivePage("household")}
        onGoToMealPlan={() => setActivePage("mealPlan")}
        profileCount={configuredProfileCount}
      />
    );
  } else if (activePage === "mealPlan") {
    pageContent = (
      <MealPlanPage
        activeProfileMeta={activeProfileMeta}
        activeProfileName={activeProfileName}
        daySelector={daySelectorNode}
        generationControls={generationControls}
        groceryContent={groceryContent}
        hasMembers={hasMembers}
        hasPlan={hasCurrentPlan}
        isSetupComplete={isSetupComplete}
        mealPlanContent={generationMode === "household" ? householdPlanContent : individualPlanContent}
        messagesContent={messagesContent}
        onGoToHousehold={() => setActivePage("household")}
        onSelectTab={setMealPlanTab}
        profileSelector={profileSelectorNode}
        selectedTab={mealPlanTab}
      />
    );
  } else if (activePage === "insights") {
    pageContent = (
      <InsightsPage
        activeProfileMeta={activeProfileMeta}
        activeProfileName={activeProfileName}
        dayIndexes={insightsDayIndexes.length ? insightsDayIndexes : [1]}
        hasMembers={hasMembers}
        hasPlan={hasCurrentPlan}
        householdName={householdName}
        isSetupComplete={isSetupComplete}
        mealContributions={insightMealContributions}
        onGoToHousehold={() => setActivePage("household")}
        onGoToMealPlan={() => setActivePage("mealPlan")}
        onSelectDay={handleInsightsDaySelect}
        profileSelector={profileSelectorNode}
        selectedDay={safeInsightsDay}
        targetTotals={insightsTargetTotals}
        totals={insightsTotals}
      />
    );
  } else {
    pageContent = (
      <HouseholdPage
        accountEmail={authAccount?.email}
        appSettingsContent={appSettingsContent}
        authError={authError}
        authMessage={authMessage}
        backendStatusText={backendStatusText}
        defaultViewerContent={defaultViewerContent}
        feedbackToolsContent={feedbackToolsContent}
        householdManagementContent={householdManagementContent}
        isAuthLoading={isAuthLoading}
        isContinuingSample={isContinuingDemo}
        isSetupComplete={isSetupComplete}
        messagesContent={messagesContent}
        onContinueAsSample={continueAsDemo}
        onLogin={loginLocalAccount}
        onLogout={logOutLocalSession}
        onRegister={registerLocalAccount}
        onUnavailableAction={showUnavailableAction}
      />
    );
  }

  return (
    <View style={styles.shell}>
      <View style={styles.content}>{pageContent}</View>
      <FloatingNav activePage={activePage} onSelectPage={setActivePage} />
    </View>
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

function DayCountSelector({
  days,
  onChange,
}: {
  days: number;
  onChange: (days: number) => void;
}) {
  const canDecrease = days > 1;
  const canIncrease = days < 5;
  return (
    <View style={styles.dayCountSelector}>
      <Text style={styles.infoLabel}>Days</Text>
      <View style={styles.dayCountControls}>
        <Pressable
          accessibilityRole="button"
          disabled={!canDecrease}
          onPress={() => onChange(Math.max(1, days - 1))}
          style={({ pressed }) => [
            styles.dayCountButton,
            pressed && canDecrease ? styles.buttonPressed : null,
            !canDecrease ? styles.buttonDisabled : null,
          ]}
        >
          <Text style={styles.dayCountArrow}>{"<"}</Text>
        </Pressable>
        <Text style={styles.dayCountValue}>{days}</Text>
        <Pressable
          accessibilityRole="button"
          disabled={!canIncrease}
          onPress={() => onChange(Math.min(5, days + 1))}
          style={({ pressed }) => [
            styles.dayCountButton,
            pressed && canIncrease ? styles.buttonPressed : null,
            !canIncrease ? styles.buttonDisabled : null,
          ]}
        >
          <Text style={styles.dayCountArrow}>{">"}</Text>
        </Pressable>
      </View>
    </View>
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

function buildProfileSelectorItems(
  household: DemoHouseholdResponse | null,
  profiles: MemberProfileResponse[],
): ProfileSelectorItem[] {
  const demoItems = (household?.members ?? []).map((member) => {
    const id = memberKey(member);
    return {
      id: `demo:${id}`,
      label: getProfileDisplayName(member),
      meta: getProfileMeta(member),
    };
  });
  const savedItems = profiles.map((profile) => ({
    id: `saved:${profile.member_profile_id}`,
    label: profile.display_name,
    meta: getProfileMeta(profile),
  }));
  return [...demoItems, ...savedItems];
}

function getProfileDisplayName(profile: DemoMemberProfile | MemberProfileResponse): string {
  return String(
    profile.display_name ??
      profile.profile_name ??
      profile.member_profile_id ??
      profile.member_id ??
      "Profile",
  );
}

function getProfileMeta(profile: DemoMemberProfile | MemberProfileResponse): string {
  const goal = formatGoal(String(profile.goal ?? "maintain"));
  const mealConfig = asRecord(profile.meal_config);
  const mealsPerDay = numberValue(mealConfig.meals_per_day) ?? 3;
  const includesSnack = Boolean(mealConfig.include_snacks ?? true);
  return `Goal: ${goal} - ${mealsPerDay} meals${includesSnack ? " + snack" : ""}`;
}

function formatGoal(goal: string): string {
  const normalized = goal.trim().toLowerCase();
  const labels: Record<string, string> = {
    gain: "Muscle gain",
    lose: "Weight loss",
    maintain: "Maintain",
  };
  return labels[normalized] ?? titleize(normalized || "maintain");
}

function getIndividualDayIndexes(
  response: IndividualPlanGenerateResponse | null,
): number[] {
  const indexes = (response?.daily_plan ?? [])
    .map((day, index) => normalizeDayIndexForUi(day.day_index, index + 1))
    .filter((value): value is number => value !== null);
  const uniqueIndexes = [...new Set(indexes)].sort((left, right) => left - right);
  return uniqueIndexes.length ? uniqueIndexes : [1];
}

function getHouseholdDayIndexes(
  response: HouseholdPlanGenerateResponse | null,
): number[] {
  const dayCount = numberValue(response?.days);
  if (dayCount !== null && dayCount > 0) {
    return Array.from({ length: Math.round(dayCount) }, (_, index) => index + 1);
  }
  const indexes = [
    ...(response?.daily_plan ?? []).map((day) => day.day_index),
    ...(response?.per_member_menus ?? []).map((menu) => menu.day_index ?? menu.day),
  ]
    .map((value, index) => normalizeDayIndexForUi(value, index + 1))
    .filter((value): value is number => value !== null);
  const uniqueIndexes = [...new Set(indexes)].sort((left, right) => left - right);
  return uniqueIndexes.length ? uniqueIndexes : [1];
}

function getIndividualDay(
  response: IndividualPlanGenerateResponse | null,
  dayIndex: number,
) {
  return (
    (response?.daily_plan ?? []).find(
      (day, index) => normalizeDayIndexForUi(day.day_index, index + 1) === dayIndex,
    ) ?? null
  );
}

function getIndividualInsightsTotals(
  response: IndividualPlanGenerateResponse | null,
  selectedDay: InsightsDaySelection,
): InsightsTotals {
  const days = response?.daily_plan ?? [];
  if (!days.length) {
    return {};
  }
  if (selectedDay === "average") {
    return averageTotals(days.map((day) => asRecord(day.totals)));
  }
  return extractTotals(asRecord(getIndividualDay(response, selectedDay)?.totals));
}

function getIndividualMealContributions(
  response: IndividualPlanGenerateResponse | null,
  selectedDay: InsightsDaySelection,
): MealContribution[] {
  const days = response?.daily_plan ?? [];
  if (!days.length) {
    return [];
  }
  if (selectedDay === "average") {
    return averageMealContributions(
      days.flatMap((day) => getMealsFromGeneratedDay(day).map((meal) => ({
        dayIndex: normalizeDayIndexForUi(day.day_index, 1) ?? 1,
        meal,
      }))),
      days.length,
    );
  }
  return getMealsFromGeneratedDay(getIndividualDay(response, selectedDay)).map(mealToContribution);
}

function getHouseholdInsightsTotals(
  response: HouseholdPlanGenerateResponse | null,
  memberId: string,
  selectedDay: InsightsDaySelection,
): InsightsTotals {
  if (!response || !memberId) {
    return {};
  }
  const dayIndexes = getHouseholdDayIndexes(response);
  if (selectedDay === "average") {
    return averageTotals(
      dayIndexes.map((dayIndex) =>
        getHouseholdTotalsForDay(response, memberId, dayIndex),
      ),
    );
  }
  return getHouseholdTotalsForDay(response, memberId, selectedDay);
}

function getHouseholdMealContributions(
  response: HouseholdPlanGenerateResponse | null,
  memberId: string,
  selectedDay: InsightsDaySelection,
): MealContribution[] {
  if (!response || !memberId) {
    return [];
  }
  const dayIndexes = getHouseholdDayIndexes(response);
  if (selectedDay === "average") {
    const rows = dayIndexes.flatMap((dayIndex) =>
      getHouseholdMealsForDay(response, memberId, dayIndex).map((meal) => ({
        dayIndex,
        meal,
      })),
    );
    return averageMealContributions(rows, dayIndexes.length);
  }
  return getHouseholdMealsForDay(response, memberId, selectedDay).map(mealToContribution);
}

function getHouseholdTargetTotals(
  response: HouseholdPlanGenerateResponse | null,
  memberId: string,
): InsightsTotals | undefined {
  const target =
    (response?.member_targets ?? []).find((item) => getRecordMemberId(item) === memberId) ??
    null;
  const summary =
    (response?.member_macro_summaries ?? []).find(
      (item) => getRecordMemberId(item) === memberId,
    ) ?? null;
  const summaryTargets = asRecord(summary?.targets);
  const totals = {
    carbs_g: numberValue(target?.carbs_g) ?? numberValue(summaryTargets.carbs_g) ?? undefined,
    fat_g: numberValue(target?.fat_g) ?? numberValue(summaryTargets.fat_g) ?? undefined,
    kcal:
      numberValue(target?.target_kcal) ??
      numberValue(target?.kcal) ??
      numberValue(summaryTargets.kcal) ??
      numberValue(summary?.target_kcal) ??
      undefined,
    protein_g:
      numberValue(target?.protein_g) ?? numberValue(summaryTargets.protein_g) ?? undefined,
  };
  return Object.values(totals).some((value) => value !== undefined) ? totals : undefined;
}

function getHouseholdTotalsForDay(
  response: HouseholdPlanGenerateResponse,
  memberId: string,
  dayIndex: number,
): InsightsTotals {
  const menu = (response.per_member_menus ?? []).find((item) => {
    const itemDayIndex = normalizeDayIndexForUi(item.day_index ?? item.day, 1);
    return getRecordMemberId(item) === memberId && itemDayIndex === dayIndex;
  });
  const summary = (response.member_macro_summaries ?? []).find((item) => {
    const itemDayIndex = normalizeDayIndexForUi(item.day_index ?? item.day, 1);
    return getRecordMemberId(item) === memberId && itemDayIndex === dayIndex;
  });
  const menuTotals = firstRecord(menu?.totals, menu?.daily_totals, menu?.macro_totals);
  const summaryTotals = asRecord(summary?.totals);
  return {
    carbs_g:
      numberValue(menuTotals.carbs_g) ??
      numberValue(summaryTotals.carbs_g) ??
      numberValue(summary?.carbs_g) ??
      undefined,
    fat_g:
      numberValue(menuTotals.fat_g) ??
      numberValue(summaryTotals.fat_g) ??
      numberValue(summary?.fat_g) ??
      undefined,
    kcal:
      numberValue(menuTotals.kcal) ??
      numberValue(summaryTotals.kcal) ??
      numberValue(summary?.kcal) ??
      undefined,
    protein_g:
      numberValue(menuTotals.protein_g) ??
      numberValue(summaryTotals.protein_g) ??
      numberValue(summary?.protein_g) ??
      undefined,
  };
}

function getHouseholdMealsForDay(
  response: HouseholdPlanGenerateResponse,
  memberId: string,
  dayIndex: number,
): GeneratedMeal[] {
  const menu = (response.per_member_menus ?? []).find((item) => {
    const itemDayIndex = normalizeDayIndexForUi(item.day_index ?? item.day, 1);
    return getRecordMemberId(item) === memberId && itemDayIndex === dayIndex;
  });
  const meals = Array.isArray(menu?.meals) ? menu?.meals : menu?.selected_meals;
  return (meals ?? []).filter(isRecord) as GeneratedMeal[];
}

function getMealsFromGeneratedDay(day: unknown): GeneratedMeal[] {
  const record = asRecord(day);
  const meals = Array.isArray(record.selected_meals)
    ? record.selected_meals
    : Array.isArray(record.meals)
      ? record.meals
      : [];
  return meals.filter(isRecord) as GeneratedMeal[];
}

function mealToContribution(meal: GeneratedMeal): MealContribution {
  return {
    kcal: numberValue(meal.kcal) ?? undefined,
    protein_g: numberValue(meal.protein_g) ?? undefined,
    slot: String(meal.slot ?? "meal"),
  };
}

function averageMealContributions(
  rows: Array<{ dayIndex: number; meal: GeneratedMeal }>,
  dayCount: number,
): MealContribution[] {
  const grouped = new Map<string, { kcal: number; protein: number }>();
  for (const row of rows) {
    const slot = String(row.meal.slot ?? "meal");
    const current = grouped.get(slot) ?? { kcal: 0, protein: 0 };
    current.kcal += numberValue(row.meal.kcal) ?? 0;
    current.protein += numberValue(row.meal.protein_g) ?? 0;
    grouped.set(slot, current);
  }
  return [...grouped.entries()].map(([slot, values]) => ({
    kcal: dayCount > 0 ? values.kcal / dayCount : values.kcal,
    protein_g: dayCount > 0 ? values.protein / dayCount : values.protein,
    slot,
  }));
}

function averageTotals(
  records: Array<Partial<InsightsTotals> | Record<string, unknown>>,
): InsightsTotals {
  const count = records.length || 1;
  const totals = records.reduce<Required<InsightsTotals>>(
    (accumulator, record) => ({
      carbs_g: accumulator.carbs_g + (numberValue(record.carbs_g) ?? 0),
      fat_g: accumulator.fat_g + (numberValue(record.fat_g) ?? 0),
      kcal: accumulator.kcal + (numberValue(record.kcal) ?? 0),
      protein_g: accumulator.protein_g + (numberValue(record.protein_g) ?? 0),
    }),
    { carbs_g: 0, fat_g: 0, kcal: 0, protein_g: 0 },
  );
  return {
    carbs_g: totals.carbs_g / count,
    fat_g: totals.fat_g / count,
    kcal: totals.kcal / count,
    protein_g: totals.protein_g / count,
  };
}

function extractTotals(record: Record<string, unknown>): InsightsTotals {
  return {
    carbs_g: numberValue(record.carbs_g) ?? undefined,
    fat_g: numberValue(record.fat_g) ?? undefined,
    kcal: numberValue(record.kcal) ?? undefined,
    protein_g: numberValue(record.protein_g) ?? undefined,
  };
}

function getRecordMemberId(value: {
  member_id?: string;
  member_profile_id?: string;
  [key: string]: unknown;
}): string {
  return String(value.member_profile_id ?? value.member_id ?? "").trim();
}

function normalizeDayIndexForUi(value: unknown, fallback: number): number | null {
  const parsed = numberValue(value);
  if (parsed === null) {
    return fallback;
  }
  return parsed <= 0 ? parsed + 1 : parsed;
}

function firstRecord(...values: unknown[]): Record<string, unknown> {
  for (const value of values) {
    if (isRecord(value) && Object.keys(value).length > 0) {
      return value;
    }
  }
  return {};
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function buildGenerateRequest(
  member: DemoMemberProfile,
  household: DemoHouseholdResponse | null,
  days: number,
): IndividualPlanGenerateRequest {
  const profileId = getMemberProfileId(member) || "demo_member";
  const householdId = getActiveHouseholdId(household, member);
  return {
    dataset_profile: "v1_2_demo_final",
    days,
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
  days: number,
): IndividualPlanGenerateRequest {
  return {
    dataset_profile: "v1_2_demo_final",
    days,
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
  days: number,
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
    days,
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

function buildSavedProfilesHouseholdGenerateRequest(
  profiles: MemberProfileResponse[],
  days: number,
): HouseholdPlanGenerateRequest {
  const householdId = getSelectedSavedProfilesHouseholdId(profiles) || DEFAULT_HOUSEHOLD_ID;
  return {
    dataset_profile: "v1_2_demo_final",
    days,
    household_id: householdId,
    selected_member_ids: profiles.map((profile) => profile.member_profile_id),
    generation_options: GENERATION_OPTIONS,
    include_grocery_list: true,
    include_purchase_suggestions: true,
    include_price_estimates: true,
    feedback_enabled: true,
    household_mode: "individual_breakfast_shared_main",
    household_allocation_mode: "macro_aware_simple",
  };
}

function getHouseholdDisplayMembers(
  source: HouseholdSource,
  demoMembers: DemoMemberProfile[],
  savedProfiles: MemberProfileResponse[],
  response: HouseholdPlanGenerateResponse | null,
): DemoMemberProfile[] {
  const responseMembers = (response?.selected_members ?? []).filter(isRecord) as DemoMemberProfile[];
  if (responseMembers.length) {
    return responseMembers;
  }
  if (source === "saved") {
    return savedProfiles.map(profileToHouseholdMember);
  }
  return demoMembers;
}

function profileToHouseholdMember(profile: MemberProfileResponse): DemoMemberProfile {
  return {
    ...profile,
    member_id: profile.member_profile_id,
    member_profile_id: profile.member_profile_id,
    profile_name: profile.display_name,
  };
}

function getSelectedSavedProfilesHouseholdId(profiles: MemberProfileResponse[]): string {
  const householdIds = profiles
    .map((profile) => String(profile.household_id ?? "").trim())
    .filter(Boolean);
  const uniqueIds = [...new Set(householdIds)];
  return uniqueIds.length === 1 ? uniqueIds[0] : "";
}

function hasMixedHouseholdIds(profiles: MemberProfileResponse[]): boolean {
  const householdIds = profiles
    .map((profile) => String(profile.household_id ?? "").trim())
    .filter(Boolean);
  return new Set(householdIds).size > 1;
}

function getActiveHouseholdId(
  household: DemoHouseholdResponse | null,
  member: DemoMemberProfile | null,
  savedProfile?: MemberProfileResponse | null,
  householdOverride?: string,
): string {
  return String(
    householdOverride ||
      savedProfile?.household_id ||
      household?.household_id ||
      member?.household_id ||
      "",
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
  shell: {
    flex: 1,
    position: "relative",
  },
  content: {
    flex: 1,
  },
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
  stack: {
    gap: 12,
  },
  messageStack: {
    gap: 8,
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
  dayCountArrow: {
    color: "#FFFFFF",
    fontSize: 18,
    fontWeight: "900",
  },
  dayCountButton: {
    alignItems: "center",
    backgroundColor: "#165D77",
    borderRadius: 8,
    height: 38,
    justifyContent: "center",
    width: 44,
  },
  dayCountControls: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
  },
  dayCountSelector: {
    alignItems: "center",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    justifyContent: "space-between",
    padding: 10,
  },
  dayCountValue: {
    color: "#111827",
    fontSize: 18,
    fontWeight: "900",
    minWidth: 28,
    textAlign: "center",
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
