import { useEffect, useMemo, useRef } from "react";
import {
  ActivityIndicator,
  Modal,
  PanResponder,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import LottieView from "lottie-react-native";

import { FloatingNav, type AppPageKey } from "../components/navigation/FloatingNav";
import { AddMemberWizard } from "../components/AddMemberWizard";
import { GroceryListSection } from "../components/GroceryListSection";
import { HouseholdMemberPlanView } from "../components/HouseholdMemberPlanView";
import { MealPlanAlternativesPrefetcher } from "../components/MealPlanAlternativesPrefetcher";
import { PlanDayCard } from "../components/PlanDayCard";
import { ProfileCard } from "../components/ProfileCard";
import { ChevronDownIcon } from "../components/icons/ChevronDownIcon";
import { AppCard } from "../components/ui/AppCard";
import { DaySelector } from "../components/ui/DaySelector";
import { ProfileSelector, type ProfileSelectorItem } from "../components/ui/ProfileSelector";
import { API_BASE_URL } from "../config/api";
import {
  buildDimaPresentationFixturePlan,
  buildDimaPresentationProgressSnapshots,
  getDimaPresentationProfileIds,
  isDimaPresentationFixtureAccount,
} from "../data/presentation/dimaPresentationFixture";
import {
  useHomeScreenState,
  type ConfirmationDialogState,
  type GenerationMode,
  type HouseholdSource,
} from "../hooks/useHomeScreenState";
import { HomePage } from "./HomePage";
import { HouseholdPage } from "./HouseholdPage";
import {
  InsightsPage,
  type DailyProgressSaveInput,
  type InsightsDaySelection,
  type InsightsTotals,
  type MealContribution,
} from "./InsightsPage";
import { MealPlanPage } from "./MealPlanPage";
import {
  clearFeedback,
  createProfile,
  deleteDailyProgress,
  deleteProfile,
  generateHouseholdPlan,
  generateIndividualPlan,
  getDailyProgress,
  getDemoHousehold,
  getFeedbackContext,
  getHealth,
  getProfiles,
  loginAccount,
  logoutAccount,
  registerAccount,
  saveDailyProgress,
  submitFeedback,
  updateHouseholdName,
} from "../services/apiClient";
import { colors } from "../theme/colors";
import type {
  AuthAccount,
  DailyProgressSnapshot,
  DemoHouseholdResponse,
  DemoMemberProfile,
  FeedbackContextResponse,
  FeedbackType,
  GeneratedMeal,
  GroceryListResponse,
  HouseholdMeal,
  HouseholdPlanGenerateRequest,
  HouseholdPlanGenerateResponse,
  IndividualPlanGenerateRequest,
  IndividualPlanGenerateResponse,
  MealReplacementResponse,
  MemberProfileCreateRequest,
  MemberProfileResponse,
} from "../types/api";

const profileEyeLottie = require("../../assets/home/eye_for_page_2.json");

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
];

const DEFAULT_HOUSEHOLD_ID = "household_demo_family_001";
const MEAL_SLOT_ORDER = ["breakfast", "lunch", "snack", "dinner"];
const HOUSEHOLD_COOKING_TIME_OPTIONS = [
  { label: "Quick", value: "quick" },
  { label: "Balanced", value: "balanced" },
  { label: "No rush", value: "no_rush" },
];

export function HomeScreen() {
  const {
    activePage,
    authAccount,
    authError,
    authMessage,
    authSessionToken,
    confirmationDialog,
    currentHouseholdMemberIndex,
    dailyProgressByKey,
    dailyProgressError,
    dailyProgressLoadingProfileId,
    defaultViewerId,
    deletingDailyProgressId,
    deletingProfileId,
    demoHousehold,
    editingMemberProfileId,
    errorMessage,
    feedbackContext,
    generatedHouseholdPlan,
    generatedPlan,
    generationMode,
    health,
    healthStatus,
    householdCookingTimePreference,
    householdErrorMessage,
    householdMessage,
    householdNameDraft,
    householdSource,
    isAddingMember,
    isAuthLoading,
    isClearingFeedback,
    isCreatingProfile,
    isEditingHouseholdName,
    isGeneratingHouseholdPlan,
    isGeneratingPlan,
    isLoadingProfiles,
    isSavingDailyProgress,
    isUpdatingHouseholdName,
    mealPlanTab,
    pendingFeedbackKey,
    planDays,
    profileErrorMessage,
    profileMessage,
    savedProfiles,
    scrollToTopRequests,
    selectedHouseholdDayIndex,
    selectedHouseholdMemberIds,
    selectedIndividualDayIndex,
    selectedInsightsDay,
    selectedMemberId,
    selectedSavedHouseholdProfileIds,
    selectedSavedProfileId,
    setActivePage,
    setAuthAccount,
    setAuthError,
    setAuthMessage,
    setAuthSessionToken,
    setConfirmationDialog,
    setCurrentHouseholdMemberIndex,
    setDailyProgressByKey,
    setDailyProgressError,
    setDailyProgressLoadingProfileId,
    setDailyProgressMessage,
    setDefaultViewerId,
    setDeletingDailyProgressId,
    setDeletingProfileId,
    setDemoHousehold,
    setEditingMemberProfileId,
    setErrorMessage,
    setFeedbackContext,
    setFeedbackError,
    setGeneratedHouseholdPlan,
    setGeneratedPlan,
    setGenerationMode,
    setHealth,
    setHealthStatus,
    setHouseholdCookingTimePreference,
    setHouseholdErrorMessage,
    setHouseholdMessage,
    setHouseholdNameDraft,
    setHouseholdSource,
    setIsAddingMember,
    setIsAuthLoading,
    setIsClearingFeedback,
    setIsContinuingDemo,
    setIsCreatingProfile,
    setIsDemoModeEnabled,
    setIsEditingHouseholdName,
    setIsGeneratingHouseholdPlan,
    setIsGeneratingPlan,
    setIsLoadingFeedbackContext,
    setIsLoadingHousehold,
    setIsLoadingProfiles,
    setIsSavingDailyProgress,
    setIsUpdatingHouseholdName,
    setMealPlanTab,
    setPendingFeedbackKey,
    setPlanDays,
    setProfileErrorMessage,
    setProfileMessage,
    setSavedProfiles,
    setScrollToTopRequests,
    setSelectedHouseholdDayIndex,
    setSelectedHouseholdMemberIds,
    setSelectedIndividualDayIndex,
    setSelectedInsightsDay,
    setSelectedMemberId,
    setSelectedSavedHouseholdProfileIds,
    setSelectedSavedProfileId,
    setToastMessage,
    toastMessage,
  } = useHomeScreenState();

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
  const feedbackStats = getFeedbackStats(feedbackContext);
  const savedHistoryCount = Object.keys(dailyProgressByKey).length;
  const dimaPresentationFixtureEnabled = isDimaPresentationFixtureAccount(authAccount);

  useEffect(() => {
    setHouseholdNameDraft(
      getHouseholdBaseName(authAccount?.household_display_name ?? "My Household"),
    );
  }, [authAccount?.household_display_name]);

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

  function selectFloatingNavPage(page: AppPageKey) {
    if (page === activePage) {
      if (page !== "household") {
        setScrollToTopRequests((current) => ({
          ...current,
          [page]: current[page] + 1,
        }));
      }
      return;
    }

    setActivePage(page);
  }

  async function loadDemoHousehold() {
    setIsLoadingHousehold(true);
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setGeneratedPlan(null);
    setGeneratedHouseholdPlan(null);
    setFeedbackContext(null);
    setDailyProgressByKey({});
    setDailyProgressMessage("");
    setDailyProgressError("");
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

  async function saveHouseholdDisplayName() {
    if (!authAccount || !authSessionToken) {
      setHouseholdMessage("");
      setHouseholdErrorMessage("Log in before editing household settings.");
      return;
    }

    const displayName = formatHouseholdDisplayName(householdNameDraft);
    setIsUpdatingHouseholdName(true);
    setHouseholdMessage("");
    setHouseholdErrorMessage("");

    try {
      const response = await updateHouseholdName(displayName, authSessionToken);
      const updatedAccount = {
        ...response.account,
        household_display_name: formatHouseholdDisplayName(
          response.account.household_display_name,
        ),
      };
      setAuthAccount(updatedAccount);
      setHouseholdNameDraft(getHouseholdBaseName(updatedAccount.household_display_name));
      setIsEditingHouseholdName(false);
      setToastMessage("Household name updated.");
    } catch (error) {
      setHouseholdErrorMessage(
        formatLocalSaveError(error, "Household name update failed"),
      );
    } finally {
      setIsUpdatingHouseholdName(false);
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
      setToastMessage(request.member_profile_id ? "Member updated." : "Member added.");
      setGeneratedPlan(null);
      setFeedbackContext(null);
    } catch (error) {
      setProfileErrorMessage(formatLocalSaveError(error, "Profile save failed"));
      throw error;
    } finally {
      setIsCreatingProfile(false);
    }
  }

  async function saveMemberFromEditor(request: MemberProfileCreateRequest) {
    await createSavedProfile(request);
    setIsAddingMember(false);
    setEditingMemberProfileId("");
  }

  function closeMemberEditor() {
    setIsAddingMember(false);
    setEditingMemberProfileId("");
  }

  function confirmEditSavedProfile(profile: MemberProfileResponse) {
    setConfirmationDialog({
      title: "Edit this member?",
      body: `You can update ${profile.display_name}'s profile details.`,
      confirmLabel: "Edit",
      variant: "primary",
      onConfirm: () => {
        setIsAddingMember(false);
        setEditingMemberProfileId(profile.member_profile_id);
      },
    });
  }

  function confirmDeleteSavedProfile(profile: MemberProfileResponse) {
    setConfirmationDialog({
      title: "Remove this member?",
      body: `${profile.display_name} will no longer be used in this household.`,
      confirmLabel: "Delete",
      variant: "danger",
      onConfirm: () => {
        void deleteSavedProfile(profile);
      },
    });
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
      if (editingMemberProfileId === memberProfileId) {
        setEditingMemberProfileId("");
      }
      setToastMessage(`Member deleted: ${profile.display_name}.`);
    } catch (error) {
      setProfileErrorMessage(formatLocalSaveError(error, "Profile remove failed"));
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
    setFeedbackError("");
    setDailyProgressMessage("");
    setDailyProgressError("");

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
      setHouseholdErrorMessage("Open a household before generating this plan.");
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
    setFeedbackError("");
    setDailyProgressMessage("");
    setDailyProgressError("");

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

  async function generateMealPlan() {
    if (!savedProfiles.length) {
      setErrorMessage("Add a member profile before generating a meal plan.");
      return;
    }

    const orderedProfiles = orderSavedProfilesForViewing(savedProfiles, defaultViewerId);
    const selectedProfile = orderedProfiles[0];
    if (dimaPresentationFixtureEnabled) {
      const activated = activateDimaPresentationFixture(orderedProfiles);
      if (activated) {
        return;
      }
    }
    setFeedbackError("");
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setDailyProgressMessage("");
    setDailyProgressError("");

    if (orderedProfiles.length === 1) {
      setGenerationMode("individual");
      setHouseholdSource("saved");
      setSelectedSavedProfileId(selectedProfile.member_profile_id);
      setSelectedSavedHouseholdProfileIds([selectedProfile.member_profile_id]);
      setSelectedMemberId("");
      setIsGeneratingPlan(true);
      setGeneratedPlan(null);
      setGeneratedHouseholdPlan(null);

      try {
        const response = await generateIndividualPlan(
          buildSavedProfileGenerateRequest(selectedProfile, planDays),
        );
        setGeneratedPlan(response);
        setSelectedIndividualDayIndex(1);
        setSelectedInsightsDay(1);
        setMealPlanTab("mealPlan");
        if (response.status === "blocked") {
          setErrorMessage("This profile needs a review before a meal plan can be generated.");
        }
      } catch (error) {
        setErrorMessage(error instanceof Error ? error.message : "Generate failed");
      } finally {
        setIsGeneratingPlan(false);
      }
      return;
    }

    setGenerationMode("household");
    setHouseholdSource("saved");
    setSelectedMemberId("");
    setSelectedSavedProfileId(selectedProfile.member_profile_id);
    setSelectedSavedHouseholdProfileIds(
      orderedProfiles.map((profile) => profile.member_profile_id),
    );
    setCurrentHouseholdMemberIndex(0);
    setSelectedHouseholdDayIndex(1);
    setIsGeneratingHouseholdPlan(true);
    setGeneratedPlan(null);
    setGeneratedHouseholdPlan(null);

    try {
      const response = await generateHouseholdPlan(
        buildSavedProfilesHouseholdGenerateRequest(orderedProfiles, planDays),
      );
      setGeneratedHouseholdPlan(response);
      setCurrentHouseholdMemberIndex(0);
      setSelectedHouseholdDayIndex(1);
      setSelectedInsightsDay(1);
      setMealPlanTab("mealPlan");
      if (response.status === "blocked") {
        setHouseholdErrorMessage("The household plan needs a review before it can be generated.");
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

  function getFeedbackMemberProfileId(): string {
    if (generationMode === "household" && currentHouseholdMember) {
      return memberKey(currentHouseholdMember);
    }
    return activeMemberProfileId;
  }

  function getFeedbackPlanId(): string | undefined {
    if (generationMode === "household" && generatedHouseholdPlan) {
      return getHouseholdPlanId(generatedHouseholdPlan);
    }
    return generatedPlan ? getPlanIdFromResponse(generatedPlan) : undefined;
  }

  function getFeedbackDayIndex(): number {
    return generationMode === "household"
      ? selectedHouseholdDayIndex
      : selectedIndividualDayIndex;
  }

  function buildMealFeedbackEventId(meal: GeneratedMeal): string {
    return buildStableFeedbackEventId({
      householdId: activeHouseholdId,
      memberProfileId: getFeedbackMemberProfileId(),
      planId: getFeedbackPlanId() ?? "",
      recipeId: getMealRecipeId(meal),
      slot: getMealSlot(meal),
      dayIndex: getFeedbackDayIndex(),
    });
  }

  async function refreshFeedbackContext(quiet = false) {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before refreshing feedback context.");
      return;
    }

    setIsLoadingFeedbackContext(true);
    if (!quiet) {
      setFeedbackError("");
    }

    try {
      const context = await getFeedbackContext(activeHouseholdId, getFeedbackMemberProfileId());
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

    setConfirmationDialog({
      title: "Clear feedback?",
      body: "This removes saved feedback events for the active household.",
      confirmLabel: "Clear",
      variant: "danger",
      onConfirm: () => {
        void clearFeedbackForActiveHousehold();
      },
    });
  }

  async function clearFeedbackForActiveHousehold() {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before clearing feedback.");
      return;
    }

    setIsClearingFeedback(true);
    setFeedbackError("");

    try {
      await clearFeedback(activeHouseholdId, undefined, true);
      await refreshFeedbackContext(true);
      setToastMessage("Feedback cleared.");
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback clear failed");
    } finally {
      setIsClearingFeedback(false);
    }
  }

  function confirmResetFeedbackCategory(
    label: string,
    feedbackType: FeedbackType,
  ) {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before resetting feedback.");
      return;
    }

    setConfirmationDialog({
      title: `Reset ${label.toLowerCase()} feedback?`,
      body: `This will clear saved ${label.toLowerCase()} recipe feedback for the current context. This action cannot be undone.`,
      confirmLabel: "Reset",
      variant: "danger",
      onConfirm: () => {
        void resetFeedbackCategory(feedbackType);
      },
    });
  }

  async function resetFeedbackCategory(feedbackType: FeedbackType) {
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before resetting feedback.");
      return;
    }

    setIsClearingFeedback(true);
    setFeedbackError("");

    try {
      await clearFeedback(
        activeHouseholdId,
        getFeedbackMemberProfileId() || undefined,
        true,
        { feedbackType },
      );
      await refreshFeedbackContext(true);
      setToastMessage("Feedback reset.");
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback reset failed");
    } finally {
      setIsClearingFeedback(false);
    }
  }

  function confirmClearSavedHistory() {
    if (!Object.keys(dailyProgressByKey).length) {
      setToastMessage("No saved history loaded.");
      return;
    }

    setConfirmationDialog({
      title: "Clear saved history?",
      body: "This will remove saved previous days currently loaded on this device.",
      confirmLabel: "Clear",
      variant: "danger",
      onConfirm: () => {
        void clearSavedProgressHistory();
      },
    });
  }

  async function clearSavedProgressHistory() {
    const snapshots = Object.values(dailyProgressByKey);
    if (!snapshots.length) {
      setToastMessage("No saved history loaded.");
      return;
    }

    setDailyProgressError("");
    setDeletingDailyProgressId("history");

    try {
      if (authSessionToken) {
        for (const snapshot of snapshots) {
          if (snapshot.progress_id) {
            await deleteDailyProgress(snapshot.progress_id, authSessionToken);
          }
        }
      }
      setDailyProgressByKey({});
      setToastMessage("Saved history cleared.");
    } catch (error) {
      setDailyProgressError(
        formatLocalSaveError(error, "Saved history clear failed"),
      );
    } finally {
      setDeletingDailyProgressId("");
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
    const eventId = buildMealFeedbackEventId(meal);
    const feedbackKey = buildFeedbackKey(eventId, feedbackType);
    const feedbackMemberProfileId = getFeedbackMemberProfileId();
    const feedbackPlanId = getFeedbackPlanId();
    setPendingFeedbackKey(feedbackKey);
    setFeedbackError("");

    try {
      await submitFeedback({
        event_id: eventId,
        household_id: activeHouseholdId,
        member_profile_id: feedbackMemberProfileId || undefined,
        plan_id: feedbackPlanId,
        recipe_id: recipeId,
        slot: slot || undefined,
        feedback_type: feedbackType,
        source: "mobile",
      });
      await refreshFeedbackContext(true);
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback save failed");
    } finally {
      setPendingFeedbackKey("");
    }
  }

  async function undoMealFeedback(meal: GeneratedMeal, feedbackType: FeedbackType) {
    const recipeId = getMealRecipeId(meal);
    if (!recipeId) {
      setFeedbackError("Feedback unavailable for this meal.");
      return;
    }
    if (!activeHouseholdId) {
      setFeedbackError("Load a household before removing feedback.");
      return;
    }

    const eventId = buildMealFeedbackEventId(meal);
    const feedbackKey = buildFeedbackKey(eventId, feedbackType);
    setPendingFeedbackKey(feedbackKey);
    setFeedbackError("");

    try {
      await clearFeedback(activeHouseholdId, getFeedbackMemberProfileId() || undefined, true, {
        eventId,
      });
      await refreshFeedbackContext(true);
    } catch (error) {
      setFeedbackError(error instanceof Error ? error.message : "Feedback undo failed");
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
      setGeneratedHouseholdPlan(
        markAppliedHouseholdReplacementAsIndividual(
          updatedPlan as HouseholdPlanGenerateResponse,
          response,
        ),
      );
    } else {
      setGeneratedPlan(updatedPlan as IndividualPlanGenerateResponse);
    }
    setFeedbackError("");
  }

  function getPendingFeedbackType(meal: GeneratedMeal): FeedbackType | null {
    if (!pendingFeedbackKey) {
      return null;
    }
    for (const feedbackType of FEEDBACK_TYPES) {
      if (pendingFeedbackKey === buildFeedbackKey(buildMealFeedbackEventId(meal), feedbackType)) {
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
    setHouseholdMessage("");
    setHouseholdErrorMessage("");

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
    setHouseholdMessage("");
    setHouseholdErrorMessage("");

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
    setHouseholdMessage("");
    setHouseholdErrorMessage("");

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
    setFeedbackError("");
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setHouseholdMessage("");
    setProfileMessage("");
    setSavedProfiles([]);
    setSelectedSavedProfileId("");
    setDefaultViewerId("");
    setSelectedSavedHouseholdProfileIds([]);
    setActivePage("household");
    void loadProfilesAfterAuth(account.household_id, sessionToken, account);
  }

  async function loadProfilesAfterAuth(
    householdId: string,
    sessionToken: string,
    account?: AuthAccount,
  ) {
    setIsLoadingProfiles(true);

    try {
      const profiles = await getProfiles(householdId, sessionToken);
      setSavedProfiles(profiles);
      if (isDimaPresentationFixtureAccount(account ?? null)) {
        const activated = activateDimaPresentationFixture(profiles, "home");
        if (activated) {
          return;
        }
      }
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
    } catch (error) {
      setProfileErrorMessage(error instanceof Error ? error.message : "Profiles fetch failed");
    } finally {
      setIsLoadingProfiles(false);
    }
  }

  function activateDimaPresentationFixture(
    profiles: MemberProfileResponse[],
    nextPage?: AppPageKey,
  ): boolean {
    const fixturePlan = buildDimaPresentationFixturePlan(profiles);
    if (!fixturePlan) {
      return false;
    }
    const profileIds = getDimaPresentationProfileIds(profiles);
    const defaultProfileId = profileIds[0] ?? "";
    const progressSnapshots = buildDimaPresentationProgressSnapshots(
      profiles,
      String(fixturePlan.household_id || activeHouseholdId || ""),
    );
    const progressByKey = Object.fromEntries(
      progressSnapshots.map((snapshot) => [
        buildDailyProgressKey(
          snapshot.member_profile_id,
          snapshot.plan_id,
          snapshot.day_index,
        ),
        snapshot,
      ]),
    );
    setPlanDays(3);
    setGenerationMode("household");
    setHouseholdSource("saved");
    setSelectedMemberId("");
    setSelectedSavedProfileId(defaultProfileId);
    setDefaultViewerId(defaultProfileId ? `saved:${defaultProfileId}` : "");
    setSelectedSavedHouseholdProfileIds(profileIds);
    setCurrentHouseholdMemberIndex(0);
    setSelectedHouseholdDayIndex(1);
    setSelectedInsightsDay(1);
    setMealPlanTab("mealPlan");
    setGeneratedPlan(null);
    setGeneratedHouseholdPlan(fixturePlan);
    setDailyProgressByKey(progressByKey);
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setDailyProgressError("");
    setDailyProgressMessage("");
    if (nextPage) {
      setActivePage(nextPage);
    }
    return true;
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
    setDailyProgressByKey({});
    setDailyProgressLoadingProfileId("");
    setIsSavingDailyProgress(false);
    setDeletingDailyProgressId("");
    setDailyProgressMessage("");
    setDailyProgressError("");
    setPendingFeedbackKey("");
    setFeedbackError("");
    setErrorMessage("");
    setHouseholdErrorMessage("");
    setHouseholdMessage("");
    setProfileMessage("");
    setProfileErrorMessage("");
    setHouseholdCookingTimePreference("balanced");
    setIsDemoModeEnabled(false);
    setDefaultViewerId("");
    setMealPlanTab("mealPlan");
    setSelectedIndividualDayIndex(1);
    setSelectedInsightsDay(1);
    setActivePage("household");
    setToastMessage("Logged out.");
  }

  function selectProfileFromSelector(profileId: string) {
    setDefaultViewerId(profileId);
    if (profileId.startsWith("saved:")) {
      const memberProfileId = profileId.replace("saved:", "");
      selectSavedProfile(memberProfileId);
      const householdIndex = selectedHouseholdDisplayMembers.findIndex(
        (member) => memberKey(member) === memberProfileId,
      );
      if (householdIndex >= 0) {
        setCurrentHouseholdMemberIndex(householdIndex);
      }
      return;
    }
    if (profileId.startsWith("demo:")) {
      selectDemoMember(profileId.replace("demo:", ""));
    }
  }

  function handleInsightsDaySelect(value: InsightsDaySelection) {
    setSelectedInsightsDay(value);
    setDailyProgressMessage("");
    setDailyProgressError("");
  }

  async function refreshDailyProgressForProfile(memberProfileId: string, quiet = false) {
    if (!authSessionToken || !memberProfileId) {
      return;
    }
    if (dimaPresentationFixtureEnabled) {
      return;
    }
    setDailyProgressLoadingProfileId(memberProfileId);
    if (!quiet) {
      setDailyProgressMessage("");
      setDailyProgressError("");
    }

    try {
      const response = await getDailyProgress(memberProfileId, authSessionToken);
      setDailyProgressByKey((current) => {
        const next = { ...current };
        for (const [key, snapshot] of Object.entries(next)) {
          if (snapshot.member_profile_id === memberProfileId) {
            delete next[key];
          }
        }
        for (const snapshot of response.snapshots ?? []) {
          next[buildDailyProgressKey(
            snapshot.member_profile_id,
            snapshot.plan_id,
            snapshot.day_index,
          )] = snapshot;
        }
        return next;
      });
    } catch (error) {
      if (!quiet) {
        setDailyProgressError(
          error instanceof Error ? error.message : "Progress history fetch failed",
        );
      }
    } finally {
      setDailyProgressLoadingProfileId((current) =>
        current === memberProfileId ? "" : current,
      );
    }
  }

  async function saveInsightsDailyProgress(input: DailyProgressSaveInput) {
    if (!authSessionToken) {
      setDailyProgressError("Log in to save progress.");
      return;
    }
    if (safeInsightsDay === "average") {
      setDailyProgressError("Average mode cannot be saved as a day.");
      return;
    }
    if (!insightsMemberProfileId || !insightsPlanId || !insightsHouseholdId) {
      setDailyProgressError("Generate a plan for a saved profile before saving progress.");
      return;
    }

    setIsSavingDailyProgress(true);
    setDailyProgressError("");
    setDailyProgressMessage("");

    try {
      const response = await saveDailyProgress(
        {
          consumed: input.consumed,
          day_index: safeInsightsDay,
          day_snapshot: input.day_snapshot,
          household_id: insightsHouseholdId,
          meal_completion: input.meal_completion,
          member_profile_id: insightsMemberProfileId,
          plan_id: insightsPlanId,
          planned: input.planned,
          target: input.target,
        },
        authSessionToken,
      );
      const snapshot = response.snapshot;
      setDailyProgressByKey((current) => ({
        ...current,
        [buildDailyProgressKey(
          snapshot.member_profile_id,
          snapshot.plan_id,
          snapshot.day_index,
        )]: snapshot,
      }));
      setDailyProgressMessage(
        response.already_saved || response.status === "already_saved"
          ? "This day is already saved."
          : "Day saved.",
      );
      void refreshDailyProgressForProfile(insightsMemberProfileId, true);
    } catch (error) {
      setDailyProgressError(error instanceof Error ? error.message : "Progress save failed");
    } finally {
      setIsSavingDailyProgress(false);
    }
  }

  async function deleteInsightsDailyProgress(progressId: string) {
    if (!authSessionToken || !progressId) {
      return;
    }

    setDeletingDailyProgressId(progressId);
    setDailyProgressError("");
    setDailyProgressMessage("");

    try {
      const response = await deleteDailyProgress(progressId, authSessionToken);
      setDailyProgressByKey((current) => {
        const next = { ...current };
        for (const [key, snapshot] of Object.entries(next)) {
          if (snapshot.progress_id === progressId || snapshot.progress_id === response.progress_id) {
            delete next[key];
          }
        }
        return next;
      });
      setDailyProgressMessage("Saved day deleted.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Progress delete failed";
      if (message.includes("HTTP 404")) {
        setDailyProgressByKey((current) => {
          const next = { ...current };
          for (const [key, snapshot] of Object.entries(next)) {
            if (snapshot.progress_id === progressId) {
              delete next[key];
            }
          }
          return next;
        });
        setDailyProgressMessage("Saved day deleted.");
        return;
      }
      setDailyProgressError(message);
    } finally {
      setDeletingDailyProgressId("");
    }
  }

  const isSetupComplete = Boolean(authAccount);
  const configuredProfileCount = savedProfiles.length;
  const hasMembers = configuredProfileCount > 0;
  const hasCurrentPlan = Boolean(generatedPlan || generatedHouseholdPlan);
  const profileSelectorItems = buildProfileSelectorItems(savedProfiles);
  const currentHouseholdMemberProfileId = currentHouseholdMember
    ? memberKey(currentHouseholdMember)
    : "";
  const selectedProfileSelectorId =
    generationMode === "household" && currentHouseholdMemberProfileId
      ? `saved:${currentHouseholdMemberProfileId}`
      : selectedSavedProfile
      ? `saved:${selectedSavedProfile.member_profile_id}`
      : defaultViewerId || profileSelectorItems[0]?.id || "";
  const selectedProfileItem =
    profileSelectorItems.find((item) => item.id === selectedProfileSelectorId) ??
    profileSelectorItems[0] ??
    null;
  const activeProfileName = selectedProfileItem?.label ?? "there";
  const activeProfileMeta =
    selectedProfileItem?.meta ?? "Goal: Muscle gain - 3 meals + snack";
  const householdName = authAccount
    ? formatHouseholdDisplayName(authAccount.household_display_name)
    : "Your Household";
  const householdBaseName = getHouseholdBaseName(householdName);
  const householdNameDraftChanged =
    householdNameDraft.trim() !== householdBaseName;
  const editingMemberProfile =
    savedProfiles.find((profile) => profile.member_profile_id === editingMemberProfileId) ??
    null;
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
      : getIndividualTargetTotals(generatedPlan);
  const insightMealContributions =
    generationMode === "household"
      ? getHouseholdMealContributions(
          generatedHouseholdPlan,
          householdInsightsMemberId,
          safeInsightsDay,
        )
      : getIndividualMealContributions(generatedPlan, safeInsightsDay);
  const insightsPlanId =
    generationMode === "household" && generatedHouseholdPlan
      ? getHouseholdPlanId(generatedHouseholdPlan)
      : generatedPlan
        ? getPlanIdFromResponse(generatedPlan)
        : undefined;
  const insightsMemberProfileId =
    generationMode === "household" ? householdInsightsMemberId : activeMemberProfileId;
  const insightsHouseholdId =
    generationMode === "household"
      ? String(generatedHouseholdPlan?.household_id || activeHouseholdId || "").trim()
      : activeHouseholdId;
  const selectedDailyProgressKey =
    safeInsightsDay === "average" || !insightsMemberProfileId || !insightsPlanId
      ? ""
      : buildDailyProgressKey(insightsMemberProfileId, insightsPlanId, safeInsightsDay);
  const selectedDailyProgress =
    selectedDailyProgressKey ? dailyProgressByKey[selectedDailyProgressKey] ?? null : null;
  const selectedProgressHistory = Object.values(dailyProgressByKey)
    .filter((snapshot) => snapshot.member_profile_id === insightsMemberProfileId)
    .sort(compareDailyProgressSnapshotsAscending);
  const dailyProgressUnavailableReason = !authSessionToken
    ? "Log in to save progress."
    : !insightsMemberProfileId
      ? "Select a saved profile to save progress."
      : !insightsPlanId
        ? "Generate a meal plan before saving progress."
        : !insightsHouseholdId
          ? "Household context is missing."
          : "";
  const isLoadingSelectedDailyProgress =
    Boolean(insightsMemberProfileId) &&
    dailyProgressLoadingProfileId === insightsMemberProfileId;

  useEffect(() => {
    if (!authSessionToken || !hasCurrentPlan || !insightsMemberProfileId) {
      return;
    }
    void refreshDailyProgressForProfile(insightsMemberProfileId, true);
  }, [authSessionToken, hasCurrentPlan, insightsMemberProfileId]);

  const profileSelectorNode = profileSelectorItems.length ? (
    <ProfileSelector
      items={profileSelectorItems.map((item) => ({ id: item.id, label: item.label }))}
      onSelect={selectProfileFromSelector}
      selectedId={selectedProfileSelectorId}
    />
  ) : null;
  const mealPlanProfileSelectorNode = profileSelectorItems.length ? (
    <MealPlanProfileSelector
      items={profileSelectorItems}
      onSelect={selectProfileFromSelector}
      selectedId={selectedProfileSelectorId}
    />
  ) : null;

  const messagesContent =
    errorMessage ||
    householdErrorMessage ||
    householdMessage ||
    profileMessage ||
    profileErrorMessage ? (
      <View style={styles.messageStack}>
        {errorMessage ? <Text style={styles.errorText}>{errorMessage}</Text> : null}
        {householdErrorMessage ? (
          <Text style={styles.errorText}>{householdErrorMessage}</Text>
        ) : null}
        {householdMessage ? <Text style={styles.successText}>{householdMessage}</Text> : null}
        {profileMessage ? <Text style={styles.successText}>{profileMessage}</Text> : null}
        {profileErrorMessage ? (
          <Text style={styles.errorText}>{profileErrorMessage}</Text>
        ) : null}
      </View>
    ) : null;

  const generationControls = (
    <View style={styles.generateStack}>
      <DayCountSelector days={planDays} onChange={setPlanDays} />
      <ActionButton
        disabled={!savedProfiles.length || isGeneratingPlan || isGeneratingHouseholdPlan}
        loading={isGeneratingPlan || isGeneratingHouseholdPlan}
        label={hasCurrentPlan ? "Regenerate" : "Generate meal plan"}
        onPress={generateMealPlan}
      />
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
            onUndoFeedback={undoMealFeedback}
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
        {currentHouseholdMember ? (
          <HouseholdMemberPlanView
            datasetProfile={generatedHouseholdPlan.dataset_profile ?? "v1_2_demo_final"}
            feedbackDisabled={Boolean(pendingFeedbackKey)}
            getPendingFeedbackType={getPendingFeedbackType}
            householdId={generatedHouseholdPlan.household_id || activeHouseholdId || undefined}
            memberId={memberKey(currentHouseholdMember)}
            members={selectedHouseholdDisplayMembers}
            onReplacementApplied={handleMealReplacementApplied}
            onSelectDay={setSelectedHouseholdDayIndex}
            onSubmitFeedback={submitMealFeedback}
            onUndoFeedback={undoMealFeedback}
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
        onToast={setToastMessage}
        title="Household grocery list"
      />
    ) : (
      <GroceryListSection groceryList={groceryList} onToast={setToastMessage} />
    );

  const alternativesPrefetcherNode = hasCurrentPlan && !dimaPresentationFixtureEnabled ? (
    <MealPlanAlternativesPrefetcher
      datasetProfile={
        generationMode === "household"
          ? generatedHouseholdPlan?.dataset_profile ?? "v1_2_demo_final"
          : "v1_2_demo_final"
      }
      generationMode={generationMode}
      householdId={
        generationMode === "household"
          ? generatedHouseholdPlan?.household_id || activeHouseholdId || undefined
          : activeHouseholdId || undefined
      }
      householdMembers={selectedHouseholdDisplayMembers}
      householdPlan={generatedHouseholdPlan}
      individualMemberProfile={selectedSavedProfile ? undefined : selectedMember ?? undefined}
      individualMemberProfileId={activeMemberProfileId || undefined}
      individualPlan={generatedPlan}
      planId={
        generationMode === "household"
          ? generatedHouseholdPlan
            ? getHouseholdPlanId(generatedHouseholdPlan)
            : undefined
          : generatedPlan
            ? getPlanIdFromResponse(generatedPlan)
            : undefined
      }
      preferredDayIndex={
        generationMode === "household" ? selectedHouseholdDayIndex : selectedIndividualDayIndex
      }
      preferredHouseholdMemberId={currentHouseholdMemberProfileId || undefined}
    />
  ) : null;

  const feedbackToolsContent = (
    <AppCard>
      <View
        accessibilityHint="Refresh feedback context runs automatically."
        style={styles.diagnosticsHeader}
      >
        <View style={styles.diagnosticsTitleBlock}>
          <Text style={styles.panelTitle}>Feedback context</Text>
        </View>
      </View>
      <View style={styles.feedbackRows}>
        <FeedbackDiagnosticRow
          count={feedbackStats.likedCount}
          disabled={isClearingFeedback}
          label="Liked"
          onReset={() => confirmResetFeedbackCategory("Liked", "liked")}
        />
        <FeedbackDiagnosticRow
          count={feedbackStats.dislikedCount}
          disabled={isClearingFeedback}
          label="Disliked"
          onReset={() => confirmResetFeedbackCategory("Disliked", "disliked")}
        />
        <FeedbackDiagnosticRow
          count={feedbackStats.tooLongCount}
          disabled={isClearingFeedback}
          label="Too long"
          onReset={() => confirmResetFeedbackCategory("Too long", "too_long")}
        />
        <FeedbackDiagnosticRow
          count={feedbackStats.avoidedCount}
          disabled={isClearingFeedback}
          label="Avoided"
          onReset={() => confirmResetFeedbackCategory("Avoided", "explicit_avoid")}
        />
      </View>
      <Text style={styles.mutedText}>
        Clear feedback by category or all at once.
      </Text>
      <View style={styles.diagnosticsActions}>
        <ActionButton
          disabled={!activeHouseholdId || isClearingFeedback}
          loading={isClearingFeedback}
          label="Clear all feedback"
          onPress={confirmClearFeedback}
          variant="secondary"
        />
      </View>
      <View style={styles.diagnosticsDivider} />
      <View style={styles.diagnosticsHeader}>
        <Text style={styles.panelTitle}>Saved data</Text>
        <Text style={styles.panelMeta}>
          {savedHistoryCount} saved {savedHistoryCount === 1 ? "day" : "days"}
        </Text>
      </View>
      <ActionButton
        disabled={!savedHistoryCount || deletingDailyProgressId === "history"}
        loading={deletingDailyProgressId === "history"}
        label="Clear saved previous days"
        onPress={confirmClearSavedHistory}
        variant="secondary"
      />
    </AppCard>
  );

  const householdManagementContent = (
    <View style={styles.stack}>
      <AppCard>
        <View style={styles.householdSettingsStack}>
          <Text style={styles.panelTitle}>My Household</Text>
          <View style={styles.householdNameForm}>
            <Text style={styles.label}>Name</Text>
            <TextInput
              autoCapitalize="words"
              editable={!isUpdatingHouseholdName && Boolean(authAccount)}
              onFocus={() => setIsEditingHouseholdName(true)}
              onChangeText={setHouseholdNameDraft}
              onSubmitEditing={saveHouseholdDisplayName}
              placeholder="Moraru"
              placeholderTextColor={colors.mutedSoft}
              returnKeyType="done"
              style={styles.textInput}
              value={householdNameDraft}
            />
          </View>
          {(isEditingHouseholdName || householdNameDraftChanged) ? (
            <View style={styles.inlineButtonRow}>
              <HouseholdNameActionButton
                disabled={!authAccount || isUpdatingHouseholdName || !householdNameDraftChanged}
                label="Save"
                loading={isUpdatingHouseholdName}
                onPress={saveHouseholdDisplayName}
              />
              <HouseholdNameActionButton
                disabled={isUpdatingHouseholdName}
                label="Cancel"
                onPress={() => {
                  setHouseholdNameDraft(householdBaseName);
                  setIsEditingHouseholdName(false);
                }}
                tone="danger"
              />
            </View>
          ) : null}
          <HouseholdCookingTimePreference
            onChange={setHouseholdCookingTimePreference}
            value={householdCookingTimePreference}
          />
        </View>
      </AppCard>
      <AppCard>
        <View style={styles.panelHeader}>
          <Text style={styles.panelTitle}>Member profiles</Text>
          <Text style={styles.panelMeta}>
            {savedProfiles.length ? `${savedProfiles.length}` : "0"}
          </Text>
        </View>
        <View style={styles.memberList}>
          {savedProfiles.length ? (
            savedProfiles.map((profile) => {
              const selected =
                profile.member_profile_id === selectedSavedProfileId ||
                selectedSavedHouseholdProfileIds.includes(profile.member_profile_id);
              return (
                <ProfileCard
                  deleteDisabled={deletingProfileId === profile.member_profile_id}
                  key={profile.member_profile_id}
                  onDelete={() => confirmDeleteSavedProfile(profile)}
                  onEdit={() => confirmEditSavedProfile(profile)}
                  profile={profile}
                  selected={selected}
                />
              );
            })
          ) : (
            <Text style={styles.mutedText}>No profiles yet.</Text>
          )}
        </View>
        <ActionButton
          disabled={isLoadingProfiles}
          label="Add Member"
          onPress={() => {
            setEditingMemberProfileId("");
            setIsAddingMember(true);
          }}
          variant="secondary"
        />
        <ActionButton
          disabled={!savedProfiles.length}
          label="Go to Meal Plan"
          onPress={() => setActivePage("mealPlan")}
          variant="secondary"
        />
      </AppCard>
    </View>
  );

  const defaultViewerContent = profileSelectorItems.length ? (
    <View style={styles.stack}>
      {profileSelectorNode}
    </View>
  ) : (
    <Text style={styles.mutedText}>Add or load profiles to choose a default viewer.</Text>
  );

  const developerDiagnosticsContent = (
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

  const memberEditorProfile = editingMemberProfileId ? editingMemberProfile : null;
  const memberEditorContent =
    isAddingMember || memberEditorProfile ? (
      <AddMemberWizard
        defaultHouseholdId={memberEditorProfile?.household_id || savedProfileHouseholdId}
        disabled={isCreatingProfile}
        forceOpen
        initialProfile={memberEditorProfile}
        mode={memberEditorProfile ? "edit" : "add"}
        onCancel={closeMemberEditor}
        onSubmit={saveMemberFromEditor}
      />
    ) : null;

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
        scrollToTopSignal={scrollToTopRequests.home}
      />
    );
  } else if (activePage === "mealPlan") {
    pageContent = (
      <>
        {alternativesPrefetcherNode}
        <MealPlanPage
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
          profileSelector={mealPlanProfileSelectorNode}
          scrollToTopSignal={scrollToTopRequests.mealPlan}
          selectedTab={mealPlanTab}
        />
      </>
    );
  } else if (activePage === "insights") {
    pageContent = (
      <InsightsPage
        activeProfileKey={selectedProfileSelectorId}
        activeProfileMeta={activeProfileMeta}
        activeProfileName={activeProfileName}
        dayIndexes={insightsDayIndexes.length ? insightsDayIndexes : [1]}
        hasMembers={hasMembers}
        hasPlan={hasCurrentPlan}
        householdName={householdName}
        isSetupComplete={isSetupComplete}
        isDeletingProgress={deletingDailyProgressId === selectedDailyProgress?.progress_id}
        isLoadingProgress={isLoadingSelectedDailyProgress}
        isSavingProgress={isSavingDailyProgress}
        mealContributions={insightMealContributions}
        onDeleteProgress={deleteInsightsDailyProgress}
        onGoToHousehold={() => setActivePage("household")}
        onGoToMealPlan={() => setActivePage("mealPlan")}
        onSaveProgress={saveInsightsDailyProgress}
        onSelectDay={handleInsightsDaySelect}
        profileSelector={mealPlanProfileSelectorNode}
        progressError={dailyProgressError}
        progressHistory={selectedProgressHistory}
        progressUnavailableReason={dailyProgressUnavailableReason}
        scrollToTopSignal={scrollToTopRequests.insights}
        savedProgress={selectedDailyProgress}
        selectedDay={safeInsightsDay}
        startWithMealsCompleted={dimaPresentationFixtureEnabled}
        targetTotals={insightsTargetTotals}
        totals={insightsTotals}
      />
    );
  } else {
    pageContent = (
      <HouseholdPage
        accountEmail={authAccount?.email}
        authError={authError}
        authMessage={authMessage}
        backendStatusText={backendStatusText}
        dataManagementContent={feedbackToolsContent}
        defaultViewerContent={defaultViewerContent}
        developerDiagnosticsContent={developerDiagnosticsContent}
        feedbackToolsContent={feedbackToolsContent}
        householdManagementContent={householdManagementContent}
        isAuthLoading={isAuthLoading}
        isSetupComplete={isSetupComplete}
        memberEditorContent={memberEditorContent}
        memberEditorTitle={memberEditorProfile ? "Edit Member" : "Add Member"}
        messagesContent={messagesContent}
        onCloseMemberEditor={closeMemberEditor}
        onLogin={loginLocalAccount}
        onLogout={logOutLocalSession}
        onRegister={registerLocalAccount}
      />
    );
  }

  return (
    <View style={styles.shell}>
      <View style={styles.content}>{pageContent}</View>
      <FloatingNav activePage={activePage} onSelectPage={selectFloatingNavPage} />
      <ConfirmationDialog
        dialog={confirmationDialog}
        onClose={() => setConfirmationDialog(null)}
      />
      {toastMessage ? (
        <View pointerEvents="none" style={styles.toast}>
          <Text style={styles.toastText}>{toastMessage}</Text>
        </View>
      ) : null}
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
        <ActivityIndicator color={variant === "secondary" ? colors.accent : "#FFFFFF"} />
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

function HouseholdNameActionButton({
  disabled,
  label,
  loading,
  onPress,
  tone = "accent",
}: {
  disabled?: boolean;
  label: string;
  loading?: boolean;
  onPress: () => void;
  tone?: "accent" | "danger";
}) {
  const isDanger = tone === "danger";
  return (
    <Pressable
      accessibilityRole="button"
      disabled={disabled}
      onPress={onPress}
      style={({ pressed }) => [
        styles.householdNameActionButton,
        isDanger ? styles.householdNameActionButtonDanger : null,
        disabled ? styles.buttonDisabled : null,
        pressed && !disabled ? styles.buttonPressed : null,
      ]}
    >
      {loading ? (
        <ActivityIndicator color={isDanger ? colors.danger : colors.accent} />
      ) : (
        <Text
          style={[
            styles.householdNameActionButtonText,
            isDanger ? styles.householdNameActionButtonTextDanger : null,
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

function MealPlanProfileSelector({
  items,
  onSelect,
  selectedId,
}: {
  items: ProfileSelectorItem[];
  onSelect: (id: string) => void;
  selectedId?: string;
}) {
  const selectedIndex = Math.max(
    0,
    items.findIndex((item) => item.id === selectedId),
  );
  const selected = items[selectedIndex] ?? null;

  function selectOffset(offset: number) {
    if (!items.length) {
      return;
    }
    const nextIndex = (selectedIndex + offset + items.length) % items.length;
    onSelect(items[nextIndex].id);
  }

  return (
    <View style={styles.mealPlanProfileSelector}>
      <Pressable
        accessibilityLabel="Previous profile"
        accessibilityRole="button"
        disabled={!items.length}
        onPress={() => selectOffset(-1)}
        style={({ pressed }) => [
          styles.mealPlanProfileArrow,
          pressed ? styles.buttonPressed : null,
        ]}
      >
        <View style={styles.chevronLeftSmall}>
          <ChevronDownIcon color={colors.accent} size={18} />
        </View>
      </Pressable>

      <View style={styles.mealPlanProfilePill}>
        <View style={styles.mealPlanProfileIdentity}>
          <LottieView
            autoPlay
            key={selected?.id ?? "profile-eye"}
            loop={false}
            resizeMode="contain"
            source={profileEyeLottie}
            speed={0.7}
            style={styles.mealPlanProfileEye}
          />
          <Text numberOfLines={1} style={styles.mealPlanProfileName}>
            {selected?.label ?? "No profile selected"}
          </Text>
        </View>
      </View>

      <Pressable
        accessibilityLabel="Next profile"
        accessibilityRole="button"
        disabled={!items.length}
        onPress={() => selectOffset(1)}
        style={({ pressed }) => [
          styles.mealPlanProfileArrow,
          pressed ? styles.buttonPressed : null,
        ]}
      >
        <View style={styles.chevronRightSmall}>
          <ChevronDownIcon color={colors.accent} size={18} />
        </View>
      </Pressable>
    </View>
  );
}

function HouseholdCookingTimePreference({
  onChange,
  value,
}: {
  onChange: (value: string) => void;
  value: string;
}) {
  const selectedIndex = Math.max(
    0,
    HOUSEHOLD_COOKING_TIME_OPTIONS.findIndex((option) => option.value === value),
  );
  const selected = HOUSEHOLD_COOKING_TIME_OPTIONS[selectedIndex];

  function selectOffset(offset: number) {
    const nextIndex =
      (selectedIndex + offset + HOUSEHOLD_COOKING_TIME_OPTIONS.length) %
      HOUSEHOLD_COOKING_TIME_OPTIONS.length;
    onChange(HOUSEHOLD_COOKING_TIME_OPTIONS[nextIndex].value);
  }

  return (
    <View style={styles.householdPreferenceField}>
      <Text style={styles.label}>Cooking time preference</Text>
      <View style={styles.householdPreferenceSelector}>
        <Pressable
          accessibilityLabel="Previous cooking time preference"
          accessibilityRole="button"
          onPress={() => selectOffset(-1)}
          style={({ pressed }) => [
            styles.householdPreferenceArrow,
            pressed ? styles.buttonPressed : null,
          ]}
        >
          <View style={styles.chevronLeftSmall}>
            <ChevronDownIcon color={colors.accent} size={16} />
          </View>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          onPress={() => selectOffset(1)}
          style={({ pressed }) => [
            styles.householdPreferenceValueWrap,
            pressed ? styles.buttonPressed : null,
          ]}
        >
          <Text style={styles.householdPreferenceValue}>
            {selected?.label ?? "Balanced"}
          </Text>
        </Pressable>
        <Pressable
          accessibilityLabel="Next cooking time preference"
          accessibilityRole="button"
          onPress={() => selectOffset(1)}
          style={({ pressed }) => [
            styles.householdPreferenceArrow,
            pressed ? styles.buttonPressed : null,
          ]}
        >
          <View style={styles.chevronRightSmall}>
            <ChevronDownIcon color={colors.accent} size={16} />
          </View>
        </Pressable>
      </View>
    </View>
  );
}

function FeedbackDiagnosticRow({
  count,
  disabled,
  label,
  onReset,
}: {
  count: number;
  disabled?: boolean;
  label: string;
  onReset: () => void;
}) {
  return (
    <View style={styles.feedbackDiagnosticRow}>
      <Text style={styles.feedbackDiagnosticLabel}>{label}</Text>
      <Text style={styles.feedbackDiagnosticCount}>{count}</Text>
      <Pressable
        accessibilityRole="button"
        disabled={disabled || count === 0}
        onPress={onReset}
        style={({ pressed }) => [
          styles.feedbackResetButton,
          disabled || count === 0 ? styles.buttonDisabled : null,
          pressed && !disabled && count > 0 ? styles.buttonPressed : null,
        ]}
      >
        <Text style={styles.feedbackResetButtonText}>Reset</Text>
      </Pressable>
    </View>
  );
}

function ConfirmationDialog({
  dialog,
  onClose,
}: {
  dialog: ConfirmationDialogState;
  onClose: () => void;
}) {
  if (!dialog) {
    return null;
  }

  function confirm() {
    const action = dialog?.onConfirm;
    onClose();
    action?.();
  }

  return (
    <Modal
      animationType="fade"
      onRequestClose={onClose}
      transparent
      visible={Boolean(dialog)}
    >
      <Pressable style={styles.modalOverlay} onPress={onClose}>
        <Pressable style={styles.confirmationCard} onPress={() => undefined}>
          <Text style={styles.confirmationTitle}>{dialog.title}</Text>
          <Text style={styles.confirmationBody}>{dialog.body}</Text>
          <View style={styles.confirmationActions}>
            <Pressable
              accessibilityRole="button"
              onPress={onClose}
              style={({ pressed }) => [
                styles.confirmationButton,
                pressed ? styles.buttonPressed : null,
              ]}
            >
              <Text style={styles.confirmationCancelText}>Cancel</Text>
            </Pressable>
            <Pressable
              accessibilityRole="button"
              onPress={confirm}
              style={({ pressed }) => [
                styles.confirmationButton,
                dialog.variant === "danger"
                  ? styles.confirmationButtonDanger
                  : styles.confirmationButtonPrimary,
                pressed ? styles.buttonPressed : null,
              ]}
            >
              <Text
                style={[
                  styles.confirmationConfirmText,
                  dialog.variant === "danger"
                    ? styles.confirmationDangerText
                    : styles.confirmationPrimaryText,
                ]}
              >
                {dialog.confirmLabel}
              </Text>
            </Pressable>
          </View>
        </Pressable>
      </Pressable>
    </Modal>
  );
}

function DayCountSelector({
  days,
  onChange,
}: {
  days: number;
  onChange: (days: number) => void;
}) {
  const options = [1, 2, 3, 4, 5];
  const fillPercent = ((days - 1) / 4) * 100;
  const fillWidth = `${fillPercent}%` as `${number}%`;
  const sliderWidthRef = useRef(1);
  const panResponder = useMemo(
    () =>
      PanResponder.create({
        onMoveShouldSetPanResponder: () => true,
        onStartShouldSetPanResponder: () => true,
        onPanResponderGrant: (event) => {
          updateDaysFromSliderPosition(
            event.nativeEvent.locationX,
            sliderWidthRef.current,
            days,
            onChange,
          );
        },
        onPanResponderMove: (event) => {
          updateDaysFromSliderPosition(
            event.nativeEvent.locationX,
            sliderWidthRef.current,
            days,
            onChange,
          );
        },
      }),
    [days, onChange],
  );

  return (
    <View style={styles.dayCountSelector}>
      <View style={styles.dayCountHeader}>
        <Text style={styles.dayCountLabel}>Days</Text>
        <Text style={styles.dayCountValue}>
          {days} {days === 1 ? "day" : "days"}
        </Text>
      </View>
      <View style={styles.sliderFrame}>
        <View
          accessibilityRole="adjustable"
          accessibilityValue={{
            min: 1,
            max: 5,
            now: days,
            text: `${days} ${days === 1 ? "day" : "days"}`,
          }}
          onLayout={(event) => {
            sliderWidthRef.current = Math.max(1, event.nativeEvent.layout.width);
          }}
          style={styles.sliderShell}
          {...panResponder.panHandlers}
        >
          <View style={styles.sliderTrackLayer}>
            <View style={styles.sliderTrack}>
              <View style={[styles.sliderTrackFill, { width: fillWidth }]} />
            </View>
            <View style={styles.sliderDotRow}>
              {options.map((option) => (
                <View
                  key={`dot-${option}`}
                  style={[
                    styles.sliderDot,
                    option <= days ? styles.sliderDotActive : null,
                  ]}
                />
              ))}
            </View>
            <View style={[styles.sliderThumb, { left: fillWidth }]}>
              <View style={styles.sliderThumbCore} />
            </View>
          </View>
        </View>
        <View style={styles.sliderTouchRow}>
          {options.map((option) => {
            const stepLeft = `${((option - 1) / 4) * 100}%` as `${number}%`;
            return (
              <Pressable
                accessibilityRole="button"
                key={option}
                onPress={() => onChange(option)}
                style={({ pressed }) => [
                  styles.sliderTouchTarget,
                  { left: stepLeft },
                  pressed ? styles.buttonPressed : null,
                ]}
              >
                <Text
                  style={[
                    styles.sliderStepLabel,
                    option === days ? styles.sliderStepLabelActive : null,
                  ]}
                >
                  {option}
                </Text>
              </Pressable>
            );
          })}
        </View>
      </View>
    </View>
  );
}

function updateDaysFromSliderPosition(
  rawX: number,
  rawWidth: number,
  currentDays: number,
  onChange: (days: number) => void,
) {
  const width = Math.max(1, rawWidth);
  const clampedX = Math.min(width, Math.max(0, rawX));
  const nextDays = Math.min(5, Math.max(1, Math.round((clampedX / width) * 4) + 1));

  if (nextDays !== currentDays) {
    onChange(nextDays);
  }
}

function InfoRow({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.infoRow}>
      <Text style={styles.infoLabel}>{label}</Text>
      <Text style={styles.infoValue}>{value}</Text>
    </View>
  );
}

function buildProfileSelectorItems(profiles: MemberProfileResponse[]): ProfileSelectorItem[] {
  return profiles.map((profile) => ({
    id: `saved:${profile.member_profile_id}`,
    label: profile.display_name,
    meta: getProfileMeta(profile),
  }));
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

function compactProfileMeta(meta?: string): string | null {
  if (!meta) {
    return null;
  }
  const cleaned = meta
    .replace(/^Goal:\s*/i, "")
    .replace(/\s+\+\s+snack\b/i, "")
    .trim();
  const parts = cleaned
    .split(/\s+-\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length >= 2) {
    return `${parts[0]} \u00B7 ${parts[1]}`;
  }
  return cleaned || null;
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

function formatHouseholdDisplayName(value?: string): string {
  const cleaned = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!cleaned) {
    return "My Household";
  }
  if (cleaned.toLowerCase().endsWith("household")) {
    return cleaned;
  }
  return `${cleaned} Household`;
}

function getHouseholdBaseName(value?: string): string {
  const cleaned = String(value ?? "").replace(/\s+/g, " ").trim();
  const withoutSuffix = cleaned.replace(/\s+household$/i, "").trim();
  return withoutSuffix || "My";
}

function formatLocalSaveError(error: unknown, fallback: string): string {
  const message = error instanceof Error ? error.message : "";
  if (!message) {
    return fallback;
  }
  if (
    message.includes("HTTP ") ||
    message.includes("Cannot reach backend") ||
    message.includes("timed out")
  ) {
    return `${fallback}. Check the connection and try again.`;
  }
  return message;
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

function getIndividualTargetTotals(
  response: IndividualPlanGenerateResponse | null,
): InsightsTotals | undefined {
  if (!response) {
    return undefined;
  }
  const generatorPlan = asRecord(response.generator_plan);
  const directTarget = asRecord(response.target);
  const generatorTarget = asRecord(generatorPlan.target);
  const fallbackTotals = getIndividualInsightsTotals(response, "average");
  const totals = {
    carbs_g:
      numberValue(directTarget.carbs_g) ??
      numberValue(directTarget.target_carbs_g) ??
      numberValue(generatorTarget.carbs_g) ??
      numberValue(generatorTarget.target_carbs_g) ??
      fallbackTotals.carbs_g,
    fat_g:
      numberValue(directTarget.fat_g) ??
      numberValue(directTarget.target_fat_g) ??
      numberValue(generatorTarget.fat_g) ??
      numberValue(generatorTarget.target_fat_g) ??
      fallbackTotals.fat_g,
    kcal:
      numberValue(directTarget.kcal) ??
      numberValue(directTarget.target_kcal) ??
      numberValue(generatorTarget.kcal) ??
      numberValue(generatorTarget.target_kcal) ??
      fallbackTotals.kcal,
    protein_g:
      numberValue(directTarget.protein_g) ??
      numberValue(directTarget.target_protein_g) ??
      numberValue(generatorTarget.protein_g) ??
      numberValue(generatorTarget.target_protein_g) ??
      fallbackTotals.protein_g,
  };
  return Object.values(totals).some((value) => value !== undefined) ? totals : undefined;
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
    carbs_g:
      numberValue(target?.target_carbs_g) ??
      numberValue(target?.carbs_g) ??
      numberValue(summaryTargets.target_carbs_g) ??
      numberValue(summaryTargets.carbs_g) ??
      numberValue(summary?.target_carbs_g) ??
      undefined,
    fat_g:
      numberValue(target?.target_fat_g) ??
      numberValue(target?.fat_g) ??
      numberValue(summaryTargets.target_fat_g) ??
      numberValue(summaryTargets.fat_g) ??
      numberValue(summary?.target_fat_g) ??
      undefined,
    kcal:
      numberValue(target?.target_kcal) ??
      numberValue(target?.kcal) ??
      numberValue(summaryTargets.target_kcal) ??
      numberValue(summaryTargets.kcal) ??
      numberValue(summary?.target_kcal) ??
      undefined,
    protein_g:
      numberValue(target?.target_protein_g) ??
      numberValue(target?.protein_g) ??
      numberValue(summaryTargets.target_protein_g) ??
      numberValue(summaryTargets.protein_g) ??
      numberValue(summary?.target_protein_g) ??
      undefined,
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
  const menuMeals = (meals ?? []).filter(isRecord) as GeneratedMeal[];
  const allocationMeals = getHouseholdAllocationMealsForDay(response, memberId, dayIndex);
  return sortMealsBySlot(mergeMealsBySlot([...menuMeals, ...allocationMeals]));
}

function getHouseholdAllocationMealsForDay(
  response: HouseholdPlanGenerateResponse,
  memberId: string,
  dayIndex: number,
): GeneratedMeal[] {
  const generatorPlan = asRecord(response.generator_plan);
  const allocations = Array.isArray(generatorPlan.allocations)
    ? generatorPlan.allocations
    : [];
  return allocations
    .filter(isRecord)
    .filter((row) => {
      const rowDayIndex = normalizeDayIndexForUi(row.day_index ?? row.day, 1);
      return getRecordMemberId(row) === memberId && rowDayIndex === dayIndex;
    })
    .map(allocationRowToMeal);
}

function allocationRowToMeal(row: Record<string, unknown>): GeneratedMeal {
  return {
    carbs_g: numberValue(row.carbs_g) ?? undefined,
    cooking_steps: Array.isArray(row.cooking_steps)
      ? row.cooking_steps.map((step) => String(step))
      : undefined,
    directions_step_count: numberValue(row.directions_step_count) ?? undefined,
    display_name: stringValue(row.display_name) ?? stringValue(row.recipe) ?? undefined,
    fat_g: numberValue(row.fat_g) ?? undefined,
    ingredient_amounts: Array.isArray(row.ingredient_amounts)
      ? row.ingredient_amounts
      : undefined,
    ingredients: Array.isArray(row.ingredients)
      ? row.ingredients.map((item) => String(item))
      : undefined,
    kcal: numberValue(row.kcal) ?? undefined,
    meal_scope: stringValue(row.allocation_scope) ?? stringValue(row.meal_scope) ?? undefined,
    portion_multiplier:
      numberValue(row.portion_multiplier_member) ??
      numberValue(row.portion_multiplier) ??
      undefined,
    protein_g: numberValue(row.protein_g) ?? undefined,
    recipe_id: stringValue(row.recipe_id) ?? undefined,
    slot: stringValue(row.slot) ?? undefined,
  };
}

function mergeMealsBySlot(meals: GeneratedMeal[]): GeneratedMeal[] {
  const bySlot = new Map<string, GeneratedMeal>();
  for (const meal of meals) {
    const slot = String(meal.slot ?? "meal").trim().toLowerCase() || "meal";
    if (!bySlot.has(slot)) {
      bySlot.set(slot, meal);
    }
  }
  return [...bySlot.values()];
}

function markAppliedHouseholdReplacementAsIndividual(
  plan: HouseholdPlanGenerateResponse,
  response: MealReplacementResponse,
): HouseholdPlanGenerateResponse {
  const replacement = asRecord(response.replacement);
  const memberId = stringValue(replacement.member_id);
  const slot = stringValue(replacement.slot);
  const dayIndex = normalizeDayIndexForUi(replacement.day_index, 1);
  const alternativeMeal = asRecord(replacement.alternative_meal);
  const alternativeRecipeId = stringValue(alternativeMeal.recipe_id);

  if (!memberId || !slot || dayIndex === null) {
    return plan;
  }

  const perMemberMenus = plan.per_member_menus?.map((menu) => {
    const menuDayIndex = normalizeDayIndexForUi(menu.day_index ?? menu.day, 1);
    if (getRecordMemberId(menu) !== memberId || menuDayIndex !== dayIndex) {
      return menu;
    }

    return {
      ...menu,
      meals: markMealListReplacementScopeAsIndividual(menu.meals, slot, alternativeRecipeId),
      selected_meals: markMealListReplacementScopeAsIndividual(
        menu.selected_meals,
        slot,
        alternativeRecipeId,
      ),
    };
  });

  return {
    ...plan,
    per_member_menus: perMemberMenus,
  };
}

function markMealListReplacementScopeAsIndividual(
  meals: HouseholdMeal[] | undefined,
  slot: string,
  alternativeRecipeId: string | null,
) {
  if (!Array.isArray(meals)) {
    return meals;
  }

  return meals.map((meal) => {
    const sameSlot = stringValue(meal.slot)?.toLowerCase() === slot.toLowerCase();
    const sameRecipe =
      !alternativeRecipeId || stringValue(meal.recipe_id) === alternativeRecipeId;

    if (!sameSlot || !sameRecipe) {
      return meal;
    }

    return {
      ...meal,
      allocation_scope: "individual",
      meal_scope: "individual",
      replacement_scope: "household_member_meal",
    };
  });
}

function getMealsFromGeneratedDay(day: unknown): GeneratedMeal[] {
  const record = asRecord(day);
  const meals = Array.isArray(record.selected_meals)
    ? record.selected_meals
    : Array.isArray(record.meals)
      ? record.meals
      : [];
  return sortMealsBySlot(meals.filter(isRecord) as GeneratedMeal[]);
}

function sortMealsBySlot<T extends { slot?: unknown }>(meals: T[]): T[] {
  return [...meals].sort((left, right) => {
    const leftIndex = getMealSlotOrderIndex(left.slot);
    const rightIndex = getMealSlotOrderIndex(right.slot);
    if (leftIndex !== rightIndex) {
      return leftIndex - rightIndex;
    }
    return String(left.slot ?? "").localeCompare(String(right.slot ?? ""));
  });
}

function getMealSlotOrderIndex(slot: unknown): number {
  const normalized = String(slot ?? "").trim().toLowerCase();
  const index = MEAL_SLOT_ORDER.indexOf(normalized);
  return index >= 0 ? index : MEAL_SLOT_ORDER.length;
}

function mealToContribution(meal: GeneratedMeal): MealContribution {
  return {
    carbs_g: numberValue(meal.carbs_g) ?? undefined,
    display_name: stringValue(meal.display_name) ?? undefined,
    fat_g: numberValue(meal.fat_g) ?? undefined,
    kcal: numberValue(meal.kcal) ?? undefined,
    protein_g: numberValue(meal.protein_g) ?? undefined,
    recipe_id: stringValue(meal.recipe_id) ?? undefined,
    slot: String(meal.slot ?? "meal"),
  };
}

function averageMealContributions(
  rows: Array<{ dayIndex: number; meal: GeneratedMeal }>,
  dayCount: number,
): MealContribution[] {
  const grouped = new Map<string, { carbs: number; fat: number; kcal: number; protein: number }>();
  for (const row of rows) {
    const slot = String(row.meal.slot ?? "meal");
    const current = grouped.get(slot) ?? { carbs: 0, fat: 0, kcal: 0, protein: 0 };
    current.carbs += numberValue(row.meal.carbs_g) ?? 0;
    current.fat += numberValue(row.meal.fat_g) ?? 0;
    current.kcal += numberValue(row.meal.kcal) ?? 0;
    current.protein += numberValue(row.meal.protein_g) ?? 0;
    grouped.set(slot, current);
  }
  return [...grouped.entries()].map(([slot, values]) => ({
    carbs_g: dayCount > 0 ? values.carbs / dayCount : values.carbs,
    fat_g: dayCount > 0 ? values.fat / dayCount : values.fat,
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

function orderSavedProfilesForViewing(
  profiles: MemberProfileResponse[],
  defaultViewerId: string,
): MemberProfileResponse[] {
  const defaultProfileId = defaultViewerId.startsWith("saved:")
    ? defaultViewerId.replace("saved:", "")
    : defaultViewerId;
  if (!defaultProfileId) {
    return profiles;
  }
  const defaultProfile = profiles.find(
    (profile) => profile.member_profile_id === defaultProfileId,
  );
  if (!defaultProfile) {
    return profiles;
  }
  return [
    defaultProfile,
    ...profiles.filter((profile) => profile.member_profile_id !== defaultProfileId),
  ];
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

function buildStableFeedbackEventId({
  dayIndex,
  householdId,
  memberProfileId,
  planId,
  recipeId,
  slot,
}: {
  dayIndex: number;
  householdId: string;
  memberProfileId: string;
  planId: string;
  recipeId: string;
  slot: string;
}): string {
  const parts = [
    "meal_feedback",
    householdId,
    memberProfileId || "household",
    planId || "no_plan",
    `day_${dayIndex}`,
    slot || "meal",
    recipeId,
  ];
  return parts.map(toFeedbackEventIdPart).join("__");
}

function toFeedbackEventIdPart(value: string): string {
  const cleaned = value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_");
  return cleaned.replace(/^_+|_+$/g, "") || "unknown";
}

function buildFeedbackKey(eventId: string, feedbackType: FeedbackType): string {
  return `${eventId}:${feedbackType}`;
}

function buildDailyProgressKey(
  memberProfileId: string,
  planId: string,
  dayIndex: number,
): string {
  return [
    String(memberProfileId || "").trim(),
    String(planId || "").trim(),
    String(dayIndex || 0),
  ].join("::");
}

function compareDailyProgressSnapshotsAscending(
  left: DailyProgressSnapshot,
  right: DailyProgressSnapshot,
): number {
  const leftTime = Date.parse(left.saved_at || left.created_at || "");
  const rightTime = Date.parse(right.saved_at || right.created_at || "");
  if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) {
    return leftTime - rightTime;
  }
  if (left.plan_id !== right.plan_id) {
    return left.plan_id.localeCompare(right.plan_id);
  }
  return left.day_index - right.day_index;
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
  return null;
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
    color: colors.text,
    fontSize: 34,
    fontWeight: "800",
  },
  subtitle: {
    color: colors.muted,
    fontSize: 17,
    fontWeight: "600",
  },
  section: {
    gap: 6,
  },
  stack: {
    gap: 12,
  },
  generateStack: {
    gap: 9,
  },
  messageStack: {
    gap: 8,
  },
  label: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "700",
    textTransform: "uppercase",
  },
  url: {
    color: colors.text,
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
    color: colors.text,
    fontSize: 18,
    fontWeight: "800",
  },
  panelMeta: {
    color: colors.mutedSoft,
    fontSize: 14,
    fontWeight: "700",
    textAlign: "right",
  },
  memberList: {
    gap: 10,
  },
  householdName: {
    color: colors.text,
    flex: 1,
    fontSize: 16,
    fontWeight: "800",
  },
  householdNameForm: {
    gap: 6,
  },
  householdNameActionButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 38,
  },
  householdNameActionButtonDanger: {
    borderColor: colors.danger,
  },
  householdNameActionButtonText: {
    color: colors.accent,
    fontSize: 14,
    fontWeight: "900",
  },
  householdNameActionButtonTextDanger: {
    color: colors.danger,
  },
  householdNameReadRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
    justifyContent: "space-between",
  },
  householdPreferenceArrow: {
    alignItems: "center",
    justifyContent: "center",
    minHeight: 42,
    width: 42,
  },
  householdPreferenceField: {
    gap: 7,
  },
  householdPreferenceSelector: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    minHeight: 46,
  },
  householdPreferenceValue: {
    color: colors.text,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "center",
  },
  householdPreferenceValueWrap: {
    alignItems: "center",
    flex: 1,
    justifyContent: "center",
    minWidth: 0,
    minHeight: 44,
    paddingHorizontal: 6,
  },
  householdSettingsStack: {
    gap: 10,
  },
  inlineButtonRow: {
    flexDirection: "row",
    gap: 8,
  },
  inlineEditButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 38,
    paddingHorizontal: 14,
  },
  inlineEditButtonText: {
    color: colors.accent,
    fontSize: 14,
    fontWeight: "900",
  },
  textInput: {
    backgroundColor: "#F8FBF3",
    borderColor: "#DDEAD3",
    borderRadius: 8,
    borderWidth: 1,
    color: colors.text,
    fontSize: 16,
    fontWeight: "800",
    minHeight: 46,
    paddingHorizontal: 12,
  },
  modalOverlay: {
    backgroundColor: "rgba(17, 24, 39, 0.32)",
    flex: 1,
    justifyContent: "center",
  },
  confirmationActions: {
    flexDirection: "row",
    gap: 10,
  },
  confirmationBody: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "700",
    lineHeight: 20,
  },
  confirmationButton: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
  },
  confirmationButtonDanger: {
    borderColor: colors.danger,
  },
  confirmationButtonPrimary: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  confirmationCancelText: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "900",
  },
  confirmationCard: {
    backgroundColor: "#FFFFFF",
    borderColor: colors.border,
    borderRadius: 18,
    borderWidth: 1,
    gap: 14,
    marginHorizontal: 22,
    padding: 18,
  },
  confirmationConfirmText: {
    fontSize: 15,
    fontWeight: "900",
  },
  confirmationDangerText: {
    color: colors.danger,
  },
  confirmationPrimaryText: {
    color: "#FFFFFF",
  },
  confirmationTitle: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900",
  },
  diagnosticsActions: {
    gap: 10,
  },
  diagnosticsDivider: {
    backgroundColor: colors.border,
    height: 1,
    opacity: 0.75,
  },
  diagnosticsHeader: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  diagnosticsSmallButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 36,
    minWidth: 84,
    paddingHorizontal: 12,
  },
  diagnosticsSmallButtonText: {
    color: colors.accent,
    fontSize: 13,
    fontWeight: "900",
  },
  diagnosticsTitleBlock: {
    flex: 1,
    gap: 2,
    minWidth: 0,
  },
  feedbackDiagnosticCount: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "900",
    minWidth: 28,
    textAlign: "right",
  },
  feedbackDiagnosticLabel: {
    color: colors.text,
    flex: 1,
    fontSize: 15,
    fontWeight: "800",
  },
  feedbackDiagnosticRow: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 10,
    minHeight: 44,
    paddingHorizontal: 12,
  },
  feedbackResetButton: {
    alignItems: "center",
    borderColor: colors.danger,
    borderRadius: 8,
    borderWidth: 1,
    justifyContent: "center",
    minHeight: 30,
    minWidth: 64,
    paddingHorizontal: 10,
  },
  feedbackResetButtonText: {
    color: colors.danger,
    fontSize: 12,
    fontWeight: "900",
  },
  feedbackRows: {
    gap: 8,
  },
  modeButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 44,
    paddingHorizontal: 12,
  },
  modeButtonSelected: {
    backgroundColor: colors.accent,
  },
  modeButtonText: {
    color: colors.accent,
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
  mealPlanProfileArrow: {
    alignItems: "center",
    backgroundColor: colors.card,
    borderColor: "#DDEAD3",
    borderRadius: 16,
    borderWidth: 1,
    height: 44,
    justifyContent: "center",
    width: 44,
  },
  chevronLeftSmall: {
    transform: [{ rotate: "90deg" }],
  },
  chevronRightSmall: {
    transform: [{ rotate: "-90deg" }],
  },
  mealPlanProfileEye: {
    height: 22,
    width: 22,
  },
  mealPlanProfileIdentity: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
    justifyContent: "center",
    minWidth: 0,
  },
  mealPlanProfileName: {
    color: "#1B2430",
    flexShrink: 1,
    fontSize: 17,
    fontWeight: "900",
    lineHeight: 21,
  },
  mealPlanProfilePill: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderColor: "#DDEAD3",
    borderRadius: 18,
    borderWidth: 1,
    elevation: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 48,
    minWidth: 0,
    paddingHorizontal: 16,
    paddingVertical: 8,
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.05,
    shadowRadius: 8,
  },
  mealPlanProfileSelector: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  dayList: {
    gap: 12,
  },
  dayCountSelector: {
    alignItems: "stretch",
    backgroundColor: "#F8FBF3",
    borderColor: "#DDEAD3",
    borderRadius: 18,
    borderWidth: 1,
    gap: 7,
    paddingBottom: 10,
    paddingHorizontal: 16,
    paddingTop: 12,
  },
  dayCountHeader: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
  },
  dayCountLabel: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "800",
  },
  dayCountValue: {
    color: colors.accentDark,
    fontSize: 14,
    fontWeight: "900",
    textAlign: "right",
  },
  sliderDot: {
    backgroundColor: "#F8FBF3",
    borderColor: "#DCE9D1",
    borderRadius: 4,
    borderWidth: 1,
    height: 8,
    width: 8,
  },
  sliderDotActive: {
    backgroundColor: colors.accent,
    borderColor: colors.accent,
  },
  sliderDotRow: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
    left: 0,
    position: "absolute",
    right: 0,
    top: 11,
  },
  sliderFrame: {
    alignSelf: "center",
    overflow: "visible",
    width: "84%",
  },
  sliderShell: {
    minHeight: 30,
    position: "relative",
  },
  sliderStepLabel: {
    color: colors.mutedSoft,
    fontSize: 12,
    fontWeight: "800",
  },
  sliderStepLabelActive: {
    color: colors.accentDark,
    fontWeight: "900",
  },
  sliderTouchRow: {
    height: 22,
    marginTop: -2,
    overflow: "visible",
    position: "relative",
  },
  sliderTouchTarget: {
    alignItems: "center",
    justifyContent: "center",
    minHeight: 22,
    marginLeft: -18,
    position: "absolute",
    width: 36,
  },
  sliderThumb: {
    alignItems: "center",
    backgroundColor: colors.card,
    borderColor: colors.accent,
    borderRadius: 12,
    borderWidth: 2,
    elevation: 2,
    height: 24,
    justifyContent: "center",
    position: "absolute",
    shadowColor: "#2F4A20",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.14,
    shadowRadius: 4,
    top: 3,
    transform: [{ translateX: -12 }],
    width: 24,
  },
  sliderThumbCore: {
    backgroundColor: colors.accent,
    borderRadius: 4,
    height: 8,
    width: 8,
  },
  sliderTrack: {
    backgroundColor: "#E8F2E0",
    borderRadius: 999,
    height: 6,
    overflow: "hidden",
  },
  sliderTrackLayer: {
    justifyContent: "center",
    minHeight: 30,
    position: "relative",
  },
  sliderTrackFill: {
    backgroundColor: colors.accent,
    borderRadius: 999,
    height: 6,
  },
  statsGrid: {
    gap: 8,
  },
  button: {
    minHeight: 48,
    alignItems: "center",
    justifyContent: "center",
    borderRadius: 8,
    backgroundColor: colors.accent,
    paddingHorizontal: 16,
  },
  buttonSecondary: {
    borderWidth: 1,
    borderColor: colors.accent,
    backgroundColor: colors.card,
  },
  buttonPressed: {
    opacity: 0.82,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  toast: {
    alignSelf: "center",
    backgroundColor: "#F8FBF4",
    borderColor: "#DDEAD3",
    borderRadius: 999,
    borderWidth: 1,
    elevation: 3,
    maxWidth: "86%",
    paddingHorizontal: 12,
    paddingVertical: 6,
    position: "absolute",
    shadowColor: "#1F2933",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.08,
    shadowRadius: 8,
    top: 34,
  },
  toastText: {
    color: colors.accentDark,
    fontSize: 12,
    fontWeight: "800",
    textAlign: "center",
  },
  buttonText: {
    color: "#FFFFFF",
    fontSize: 16,
    fontWeight: "800",
  },
  buttonTextSecondary: {
    color: colors.accent,
  },
  infoRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 16,
  },
  infoLabel: {
    color: colors.muted,
    fontSize: 15,
    fontWeight: "600",
  },
  infoValue: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 15,
    fontWeight: "700",
    textAlign: "right",
  },
  mutedText: {
    color: colors.mutedSoft,
    fontSize: 15,
  },
  errorText: {
    color: colors.danger,
    fontSize: 15,
    fontWeight: "700",
  },
  successText: {
    color: colors.success,
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
