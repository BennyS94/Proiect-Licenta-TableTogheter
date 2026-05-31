import { API_BASE_URL } from "../config/api";
import type {
  DemoHouseholdResponse,
  HealthResponse,
  IndividualPlanGenerateRequest,
  IndividualPlanGenerateResponse,
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
