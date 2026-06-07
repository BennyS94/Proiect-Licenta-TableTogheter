declare const process: {
  env?: {
    EXPO_PUBLIC_API_BASE_URL?: string;
  };
};

const configuredApiBaseUrl = process.env?.EXPO_PUBLIC_API_BASE_URL?.trim();

export const API_BASE_URL = configuredApiBaseUrl || "http://10.0.2.2:8000";

export const API_URL_NOTES = {
  androidEmulator: "http://10.0.2.2:8000",
  envOverride: "EXPO_PUBLIC_API_BASE_URL=http://192.168.x.x:8000",
  physicalPhoneExample: "http://192.168.x.x:8000",
  localBrowser: "http://127.0.0.1:8000",
};
