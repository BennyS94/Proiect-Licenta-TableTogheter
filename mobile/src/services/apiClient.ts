import { API_BASE_URL } from "../config/api";
import type {
  AuthResponse,
  DeleteProfileResponse,
  DemoHouseholdResponse,
  FeedbackContextResponse,
  FeedbackDeleteResponse,
  FeedbackEventRequest,
  FeedbackEventResponse,
  HealthResponse,
  HouseholdSettingsResponse,
  HouseholdPlanGenerateRequest,
  HouseholdPlanGenerateResponse,
  IndividualPlanGenerateRequest,
  IndividualPlanGenerateResponse,
  MealReplacementRequest,
  MealReplacementResponse,
  MemberProfileCreateRequest,
  MemberProfileResponse,
  MeResponse,
  ProfilesListResponse,
  RecipeAlternativesRequest,
  RecipeAlternativesResponse,
} from "../types/api";

export type { HealthResponse } from "../types/api";

const REQUEST_TIMEOUT_MS = 15000;
const GENERATION_REQUEST_TIMEOUT_MS = 240000;
const RECIPE_ACTION_REQUEST_TIMEOUT_MS = 60000;

export async function getHealth(): Promise<HealthResponse> {
  const payload = await requestJson<Partial<HealthResponse>>("/health");
  if (payload.status !== "ok") {
    throw new Error(`Backend returned unexpected status: ${payload.status ?? "missing"}`);
  }

  return {
    status: payload.status,
    service: payload.service ?? "unknown",
    version: payload.version ?? "unknown",
    database: payload.database ?? "unknown",
  };
}

export async function getDemoHousehold(): Promise<DemoHouseholdResponse> {
  const payload = await requestJson<DemoHouseholdResponse>("/households/demo");
  if (!Array.isArray(payload.members)) {
    throw new Error("Sample household response does not include members");
  }
  return payload;
}

export async function registerAccount(
  email: string,
  password: string,
  confirmPassword: string,
): Promise<AuthResponse> {
  return requestJson<AuthResponse>("/auth/register", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email,
      password,
      confirm_password: confirmPassword,
    }),
  });
}

export async function loginAccount(
  email: string,
  password: string,
): Promise<AuthResponse> {
  return requestJson<AuthResponse>("/auth/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email,
      password,
    }),
  });
}

export async function logoutAccount(sessionToken?: string): Promise<{ status: string; message: string }> {
  return requestJson<{ status: string; message: string }>("/auth/logout", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      session_token: sessionToken ?? "",
    }),
  });
}

export async function getCurrentAccount(sessionToken: string): Promise<MeResponse> {
  return requestJson<MeResponse>("/auth/me", {
    headers: authHeaders(sessionToken),
  });
}

export async function updateHouseholdName(
  displayName: string,
  sessionToken: string,
): Promise<HouseholdSettingsResponse> {
  return requestJson<HouseholdSettingsResponse>("/households/me", {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      ...authHeaders(sessionToken),
    },
    body: JSON.stringify({
      display_name: displayName,
    }),
  });
}

export async function getProfiles(
  householdId?: string,
  sessionToken?: string,
): Promise<MemberProfileResponse[]> {
  const params = new URLSearchParams();
  if (householdId && !sessionToken) {
    params.set("household_id", householdId);
  }
  const query = params.toString();
  const payload = await requestJson<ProfilesListResponse>(
    `/profiles${query ? `?${query}` : ""}`,
    {
      headers: authHeaders(sessionToken),
    },
  );
  if (!Array.isArray(payload.profiles)) {
    throw new Error("Profiles response does not include profiles");
  }
  return payload.profiles;
}

export async function createProfile(
  request: MemberProfileCreateRequest,
  sessionToken?: string,
): Promise<MemberProfileResponse> {
  return requestJson<MemberProfileResponse>("/profiles", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...authHeaders(sessionToken),
    },
    body: JSON.stringify(request),
  });
}

export async function getProfile(
  memberProfileId: string,
  sessionToken?: string,
): Promise<MemberProfileResponse> {
  return requestJson<MemberProfileResponse>(
    `/profiles/${encodeURIComponent(memberProfileId)}`,
    {
      headers: authHeaders(sessionToken),
    },
  );
}

export async function deleteProfile(
  memberProfileId: string,
  sessionToken?: string,
): Promise<DeleteProfileResponse> {
  const encodedProfileId = encodeURIComponent(memberProfileId);
  return requestJson<DeleteProfileResponse>(
    `/profiles/${encodedProfileId}?confirm=true`,
    {
      method: "DELETE",
      headers: authHeaders(sessionToken),
    },
  );
}

export async function generateIndividualPlan(
  request: IndividualPlanGenerateRequest,
): Promise<IndividualPlanGenerateResponse> {
  return requestJson<IndividualPlanGenerateResponse>(
    "/plans/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    },
    GENERATION_REQUEST_TIMEOUT_MS,
  );
}

export async function generateHouseholdPlan(
  request: HouseholdPlanGenerateRequest,
): Promise<HouseholdPlanGenerateResponse> {
  return requestJson<HouseholdPlanGenerateResponse>(
    "/household-plans/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    },
    GENERATION_REQUEST_TIMEOUT_MS,
  );
}

export async function getRecipeAlternatives(
  request: RecipeAlternativesRequest,
): Promise<RecipeAlternativesResponse> {
  return requestJson<RecipeAlternativesResponse>(
    "/recipes/similar",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    },
    RECIPE_ACTION_REQUEST_TIMEOUT_MS,
  );
}

export async function previewMealReplacement(
  planId: string,
  request: MealReplacementRequest,
): Promise<MealReplacementResponse> {
  const encodedPlanId = encodeURIComponent(planId);
  return requestJson<MealReplacementResponse>(
    `/plans/${encodedPlanId}/replace-meal?dry_run=true`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    },
    RECIPE_ACTION_REQUEST_TIMEOUT_MS,
  );
}

export async function applyMealReplacement(
  planId: string,
  request: MealReplacementRequest,
): Promise<MealReplacementResponse> {
  const encodedPlanId = encodeURIComponent(planId);
  return requestJson<MealReplacementResponse>(
    `/plans/${encodedPlanId}/replace-meal?dry_run=false`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(request),
    },
    RECIPE_ACTION_REQUEST_TIMEOUT_MS,
  );
}

export async function submitFeedback(
  request: FeedbackEventRequest,
): Promise<FeedbackEventResponse> {
  return requestJson<FeedbackEventResponse>("/feedback", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });
}

export async function getFeedbackContext(
  householdId?: string,
  memberProfileId?: string,
): Promise<FeedbackContextResponse> {
  const params = new URLSearchParams();
  if (householdId) {
    params.set("household_id", householdId);
  }
  if (memberProfileId) {
    params.set("member_profile_id", memberProfileId);
  }
  const query = params.toString();
  return requestJson<FeedbackContextResponse>(`/feedback/context${query ? `?${query}` : ""}`);
}

export async function clearFeedback(
  householdId?: string,
  memberProfileId?: string,
  confirm = true,
): Promise<FeedbackDeleteResponse> {
  const params = new URLSearchParams();
  params.set("confirm", confirm ? "true" : "false");
  if (householdId) {
    params.set("household_id", householdId);
  }
  if (memberProfileId) {
    params.set("member_profile_id", memberProfileId);
  }
  return requestJson<FeedbackDeleteResponse>(`/feedback?${params.toString()}`, {
    method: "DELETE",
  });
}

async function requestJson<T>(
  path: string,
  init?: RequestInit,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<T> {
  const url = `${API_BASE_URL}${path}`;
  const controller =
    typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeoutError = new Error(
    `Backend request timed out after ${Math.round(timeoutMs / 1000)}s at ${url}`,
  );

  let response: Response;
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    const request = fetch(url, controller ? { ...init, signal: controller.signal } : init);
    const timeout = new Promise<never>((_, reject) => {
      timeoutId = setTimeout(() => {
        controller?.abort();
        reject(timeoutError);
      }, timeoutMs);
    });
    response = await Promise.race([request, timeout]);
  } catch (error) {
    if (error === timeoutError) {
      throw timeoutError;
    }
    const message = error instanceof Error ? error.message : "unknown_error";
    throw new Error(`Cannot reach backend at ${url}: ${message}`);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    if (path === "/auth/login" && response.status === 401) {
      throw new Error("Invalid email or password");
    }
    throw new Error(`Backend request failed with HTTP ${response.status}: ${detail}`);
  }

  return (await response.json()) as T;
}

function authHeaders(sessionToken?: string): Record<string, string> {
  return sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
}

async function readErrorDetail(response: Response): Promise<string> {
  try {
    const payload = await response.json();
    if (payload && typeof payload === "object" && "detail" in payload) {
      return String(payload.detail);
    }
    return JSON.stringify(payload);
  } catch {
    return response.statusText || "unknown_error";
  }
}
