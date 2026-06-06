import { API_BASE_URL } from "../config/api";
import type {
  DeleteProfileResponse,
  DemoHouseholdResponse,
  FeedbackContextResponse,
  FeedbackDeleteResponse,
  FeedbackEventRequest,
  FeedbackEventResponse,
  HealthResponse,
  HouseholdPlanGenerateRequest,
  HouseholdPlanGenerateResponse,
  IndividualPlanGenerateRequest,
  IndividualPlanGenerateResponse,
  MemberProfileCreateRequest,
  MemberProfileResponse,
  ProfilesListResponse,
} from "../types/api";

export type { HealthResponse } from "../types/api";

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
    throw new Error("Demo household response does not include members");
  }
  return payload;
}

export async function getProfiles(householdId?: string): Promise<MemberProfileResponse[]> {
  const params = new URLSearchParams();
  if (householdId) {
    params.set("household_id", householdId);
  }
  const query = params.toString();
  const payload = await requestJson<ProfilesListResponse>(`/profiles${query ? `?${query}` : ""}`);
  if (!Array.isArray(payload.profiles)) {
    throw new Error("Profiles response does not include profiles");
  }
  return payload.profiles;
}

export async function createProfile(
  request: MemberProfileCreateRequest,
): Promise<MemberProfileResponse> {
  return requestJson<MemberProfileResponse>("/profiles", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });
}

export async function getProfile(memberProfileId: string): Promise<MemberProfileResponse> {
  return requestJson<MemberProfileResponse>(`/profiles/${encodeURIComponent(memberProfileId)}`);
}

export async function deleteProfile(
  memberProfileId: string,
): Promise<DeleteProfileResponse> {
  const encodedProfileId = encodeURIComponent(memberProfileId);
  return requestJson<DeleteProfileResponse>(
    `/profiles/${encodedProfileId}?confirm=true`,
    {
      method: "DELETE",
    },
  );
}

export async function generateIndividualPlan(
  request: IndividualPlanGenerateRequest,
): Promise<IndividualPlanGenerateResponse> {
  return requestJson<IndividualPlanGenerateResponse>("/plans/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });
}

export async function generateHouseholdPlan(
  request: HouseholdPlanGenerateRequest,
): Promise<HouseholdPlanGenerateResponse> {
  return requestJson<HouseholdPlanGenerateResponse>("/household-plans/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });
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

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const url = `${API_BASE_URL}${path}`;

  let response: Response;
  try {
    response = await fetch(url, init);
  } catch (error) {
    const message = error instanceof Error ? error.message : "unknown_error";
    throw new Error(`Cannot reach backend at ${url}: ${message}`);
  }

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new Error(`Backend request failed with HTTP ${response.status}: ${detail}`);
  }

  return (await response.json()) as T;
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
