import { useEffect, useState } from "react";

import type { AppPageKey } from "../components/navigation/FloatingNav";
import type { InsightsDaySelection } from "../screens/InsightsPage";
import type { MealPlanTab } from "../screens/MealPlanPage";
import type {
  AuthAccount,
  DailyProgressSnapshot,
  DemoHouseholdResponse,
  FeedbackContextResponse,
  HouseholdPlanGenerateResponse,
  IndividualPlanGenerateResponse,
  MemberProfileResponse,
  HealthResponse,
} from "../types/api";

export type HealthState = "idle" | "loading" | "connected" | "error";
export type GenerationMode = "individual" | "household";
export type HouseholdSource = "demo" | "saved";
export type ConfirmationDialogState = {
  body: string;
  confirmLabel: string;
  onConfirm: () => void;
  title: string;
  variant?: "danger" | "primary";
} | null;

export function useHomeScreenState() {
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
  const [, setFeedbackError] = useState("");
  const [errorMessage, setErrorMessage] = useState("");
  const [householdErrorMessage, setHouseholdErrorMessage] = useState("");
  const [householdMessage, setHouseholdMessage] = useState("");
  const [profileMessage, setProfileMessage] = useState("");
  const [profileErrorMessage, setProfileErrorMessage] = useState("");
  const [householdCookingTimePreference, setHouseholdCookingTimePreference] =
    useState("balanced");
  const [confirmationDialog, setConfirmationDialog] =
    useState<ConfirmationDialogState>(null);
  const [toastMessage, setToastMessage] = useState("");
  const [authMessage, setAuthMessage] = useState("");
  const [authError, setAuthError] = useState("");
  const [authSessionToken, setAuthSessionToken] = useState("");
  const [authAccount, setAuthAccount] = useState<AuthAccount | null>(null);
  const [isAuthLoading, setIsAuthLoading] = useState(false);
  const [householdNameDraft, setHouseholdNameDraft] = useState("My Household");
  const [isEditingHouseholdName, setIsEditingHouseholdName] = useState(false);
  const [isUpdatingHouseholdName, setIsUpdatingHouseholdName] = useState(false);
  const [isAddingMember, setIsAddingMember] = useState(false);
  const [editingMemberProfileId, setEditingMemberProfileId] = useState("");
  const [activePage, setActivePage] = useState<AppPageKey>("home");
  const [scrollToTopRequests, setScrollToTopRequests] = useState<
    Record<AppPageKey, number>
  >({
    home: 0,
    household: 0,
    insights: 0,
    mealPlan: 0,
  });
  const [isDemoModeEnabled, setIsDemoModeEnabled] = useState(false);
  const [isContinuingDemo, setIsContinuingDemo] = useState(false);
  const [defaultViewerId, setDefaultViewerId] = useState("");
  const [mealPlanTab, setMealPlanTab] = useState<MealPlanTab>("mealPlan");
  const [selectedIndividualDayIndex, setSelectedIndividualDayIndex] = useState(1);
  const [selectedInsightsDay, setSelectedInsightsDay] =
    useState<InsightsDaySelection>(1);
  const [planDays, setPlanDays] = useState(3);
  const [dailyProgressByKey, setDailyProgressByKey] = useState<
    Record<string, DailyProgressSnapshot>
  >({});
  const [dailyProgressLoadingProfileId, setDailyProgressLoadingProfileId] =
    useState("");
  const [isSavingDailyProgress, setIsSavingDailyProgress] = useState(false);
  const [deletingDailyProgressId, setDeletingDailyProgressId] = useState("");
  const [, setDailyProgressMessage] = useState("");
  const [dailyProgressError, setDailyProgressError] = useState("");

  useEffect(() => {
    if (!toastMessage) {
      return;
    }
    const timeoutId = setTimeout(() => {
      setToastMessage("");
    }, 2600);
    return () => clearTimeout(timeoutId);
  }, [toastMessage]);

  return {
    activePage,
    authAccount,
    authError,
    authMessage,
    authSessionToken,
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
    isContinuingDemo,
    isCreatingProfile,
    isDemoModeEnabled,
    isEditingHouseholdName,
    isGeneratingHouseholdPlan,
    isGeneratingPlan,
    isLoadingFeedbackContext,
    isLoadingHousehold,
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
    confirmationDialog,
    setConfirmationDialog,
  };
}
