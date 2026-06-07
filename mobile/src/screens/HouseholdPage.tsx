import type { ReactNode } from "react";
import { useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";

import { AppButton, SecondaryButton } from "../components/ui/AppButton";
import { AppCard } from "../components/ui/AppCard";
import { AppScreen } from "../components/ui/AppScreen";
import { SectionHeader } from "../components/ui/SectionHeader";
import { colors } from "../theme/colors";

type HouseholdSection = "hub" | "account" | "household" | "app";
type AuthMode = "register" | "login";

type HouseholdPageProps = {
  accountEmail?: string;
  appSettingsContent?: ReactNode;
  authError?: string;
  authMessage?: string;
  backendStatusText: string;
  defaultViewerContent?: ReactNode;
  feedbackToolsContent?: ReactNode;
  householdManagementContent?: ReactNode;
  isAuthLoading?: boolean;
  isSetupComplete: boolean;
  messagesContent?: ReactNode;
  onLogin: (email: string, password: string) => Promise<void> | void;
  onLogout: () => Promise<void> | void;
  onRegister: (
    email: string,
    password: string,
    confirmPassword: string,
  ) => Promise<void> | void;
};

export function HouseholdPage({
  accountEmail,
  appSettingsContent,
  authError,
  authMessage,
  backendStatusText,
  defaultViewerContent,
  feedbackToolsContent,
  householdManagementContent,
  isAuthLoading,
  isSetupComplete,
  messagesContent,
  onLogin,
  onLogout,
  onRegister,
}: HouseholdPageProps) {
  const [section, setSection] = useState<HouseholdSection>("hub");

  if (!isSetupComplete) {
    return (
      <AppScreen>
        <AuthSetupCard
          authError={authError}
          authMessage={authMessage}
          isAuthLoading={isAuthLoading}
          onLogin={onLogin}
          onRegister={onRegister}
        />
        {messagesContent}
      </AppScreen>
    );
  }

  if (section === "account") {
    return (
      <AppScreen>
        <BackButton label="Household / Account" onPress={() => setSection("hub")} />
        <AppCard>
          <SectionHeader title="Account Settings" />
          <InfoLine label="Email" value={accountEmail || "Guest session"} />
          <InfoLine label="Account type" value={accountEmail ? "Local household account" : "Local session"} />
          <View style={styles.settingsStack}>
            <SettingStatusLine label="Change Email" value="Coming soon" />
            <SettingStatusLine label="Change Password" value="Coming soon" />
            <SecondaryButton label="Log Out" onPress={onLogout} />
          </View>
        </AppCard>
        {messagesContent}
      </AppScreen>
    );
  }

  if (section === "household") {
    return (
      <AppScreen>
        <BackButton label="Household / Account" onPress={() => setSection("hub")} />
        <View style={styles.section}>
          <SectionHeader title="Household Management" />
          {householdManagementContent}
        </View>
        <AppCard>
          <SectionHeader title="Default Viewer" meta="Current device user" />
          {defaultViewerContent}
        </AppCard>
        {messagesContent}
      </AppScreen>
    );
  }

  if (section === "app") {
    return (
      <AppScreen>
        <BackButton label="Household / Account" onPress={() => setSection("hub")} />
        <AppCard>
          <SectionHeader title="App Settings" />
          <SettingStatusLine label="Language" value="English" />
          <SettingStatusLine label="Appearance" value="Light - Dark mode coming soon" />
          <InfoLine label="Backend status" value={backendStatusText} />
          <InfoLine label="Version" value="1.0 Preview" />
          {appSettingsContent}
        </AppCard>
        {feedbackToolsContent}
        {messagesContent}
      </AppScreen>
    );
  }

  return (
    <AppScreen>
      <View style={styles.header}>
        <Text style={styles.pageTitle}>Household / Account</Text>
        <Text style={styles.subtitle}>Manage account, members and app preferences.</Text>
      </View>

      <View style={styles.hubList}>
        <HubRow
          description="Email, password, log out"
          onPress={() => setSection("account")}
          title="Account Settings"
        />
        <HubRow
          description="Members, default viewer, household preferences"
          onPress={() => setSection("household")}
          title="Household Management"
        />
        <HubRow
          description="Theme, language, backend status"
          onPress={() => setSection("app")}
          title="App Settings"
        />
      </View>

      {messagesContent}
    </AppScreen>
  );
}

function AuthSetupCard({
  authError,
  authMessage,
  isAuthLoading,
  onLogin,
  onRegister,
}: {
  authError?: string;
  authMessage?: string;
  isAuthLoading?: boolean;
  onLogin: (email: string, password: string) => Promise<void> | void;
  onRegister: (
    email: string,
    password: string,
    confirmPassword: string,
  ) => Promise<void> | void;
}) {
  const [mode, setMode] = useState<AuthMode>("register");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [validationError, setValidationError] = useState("");

  async function submit() {
    const cleanEmail = email.trim().toLowerCase();
    if (!cleanEmail) {
      setValidationError("Email is required.");
      return;
    }
    if (password.length < 6) {
      setValidationError("Password must have at least 6 characters.");
      return;
    }
    if (mode === "register" && password !== confirmPassword) {
      setValidationError("Passwords must match.");
      return;
    }
    setValidationError("");
    if (mode === "register") {
      await onRegister(cleanEmail, password, confirmPassword);
      return;
    }
    await onLogin(cleanEmail, password);
  }

  return (
    <AppCard>
      <View style={styles.copy}>
        <Text style={styles.title}>Welcome to TableTogether</Text>
        <Text style={styles.bodyText}>
          Create a local household account to save family profiles and generate meal plans.
        </Text>
      </View>

      <View style={styles.authModeRow}>
        <ModeButton
          label="Create Account"
          onPress={() => setMode("register")}
          selected={mode === "register"}
        />
        <ModeButton
          label="Log In"
          onPress={() => setMode("login")}
          selected={mode === "login"}
        />
      </View>

      <View style={styles.formStack}>
        <AuthField
          autoCapitalize="none"
          keyboardType="email-address"
          label="Email"
          onChangeText={setEmail}
          value={email}
        />
        <AuthField
          label="Password"
          onChangeText={setPassword}
          secureTextEntry
          value={password}
        />
        {mode === "register" ? (
          <AuthField
            label="Confirm password"
            onChangeText={setConfirmPassword}
            secureTextEntry
            value={confirmPassword}
          />
        ) : null}
      </View>

      {validationError ? <Text style={styles.errorText}>{validationError}</Text> : null}
      {authError ? <Text style={styles.errorText}>{authError}</Text> : null}
      {authMessage ? <Text style={styles.successText}>{authMessage}</Text> : null}

      <AppButton
        disabled={isAuthLoading}
        label={mode === "register" ? "Create Account" : "Log In"}
        loading={isAuthLoading}
        onPress={submit}
      />

    </AppCard>
  );
}

function AuthField({
  autoCapitalize,
  keyboardType,
  label,
  onChangeText,
  secureTextEntry,
  value,
}: {
  autoCapitalize?: "none" | "sentences" | "words" | "characters";
  keyboardType?: "default" | "email-address";
  label: string;
  onChangeText: (value: string) => void;
  secureTextEntry?: boolean;
  value: string;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      <TextInput
        autoCapitalize={autoCapitalize ?? "none"}
        keyboardType={keyboardType ?? "default"}
        onChangeText={onChangeText}
        secureTextEntry={secureTextEntry}
        style={styles.input}
        value={value}
      />
    </View>
  );
}

function HubRow({
  description,
  onPress,
  title,
}: {
  description: string;
  onPress: () => void;
  title: string;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [styles.hubRow, pressed ? styles.pressed : null]}
    >
      <View style={styles.hubText}>
        <Text style={styles.hubTitle}>{title}</Text>
        <Text style={styles.hubDescription}>{description}</Text>
      </View>
      <Text style={styles.chevron}>{">"}</Text>
    </Pressable>
  );
}

function BackButton({ label, onPress }: { label: string; onPress: () => void }) {
  return (
    <Pressable
      accessibilityRole="button"
      onPress={onPress}
      style={({ pressed }) => [styles.backButton, pressed ? styles.pressed : null]}
    >
      <Text style={styles.backText}>{"<"} {label}</Text>
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
        pressed ? styles.pressed : null,
      ]}
    >
      <Text style={[styles.modeButtonText, selected ? styles.modeButtonTextSelected : null]}>
        {label}
      </Text>
    </Pressable>
  );
}

function InfoLine({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.infoLine}>
      <Text style={styles.infoLabel}>{label}</Text>
      <Text style={styles.infoValue}>{value}</Text>
    </View>
  );
}

function SettingStatusLine({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.settingLine}>
      <Text style={styles.settingLabel}>{label}</Text>
      <Text style={styles.settingValue}>{value}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  authModeRow: {
    flexDirection: "row",
    gap: 10,
  },
  backButton: {
    alignSelf: "flex-start",
    paddingVertical: 4,
  },
  backText: {
    color: colors.accent,
    fontSize: 15,
    fontWeight: "900",
  },
  bodyText: {
    color: "#4B5563",
    fontSize: 15,
    lineHeight: 21,
  },
  buttonStack: {
    gap: 10,
  },
  chevron: {
    color: colors.accent,
    fontSize: 24,
    fontWeight: "900",
  },
  copy: {
    gap: 8,
  },
  errorText: {
    color: "#B42318",
    fontSize: 14,
    fontWeight: "800",
  },
  field: {
    gap: 6,
  },
  fieldLabel: {
    color: "#4B5563",
    fontSize: 13,
    fontWeight: "800",
    textTransform: "uppercase",
  },
  formStack: {
    gap: 10,
  },
  header: {
    gap: 4,
    paddingTop: 4,
  },
  hubDescription: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "700",
  },
  hubList: {
    gap: 10,
  },
  hubRow: {
    alignItems: "center",
    backgroundColor: colors.card,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
    padding: 14,
  },
  hubText: {
    flex: 1,
    gap: 4,
  },
  hubTitle: {
    color: "#111827",
    fontSize: 17,
    fontWeight: "900",
  },
  infoLabel: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "700",
  },
  infoLine: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  infoValue: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
  input: {
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    color: "#111827",
    fontSize: 15,
    minHeight: 44,
    paddingHorizontal: 12,
  },
  modeButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 42,
    paddingHorizontal: 10,
  },
  modeButtonSelected: {
    backgroundColor: colors.accent,
  },
  modeButtonText: {
    color: colors.accent,
    fontSize: 14,
    fontWeight: "900",
    textAlign: "center",
  },
  modeButtonTextSelected: {
    color: "#FFFFFF",
  },
  mutedText: {
    color: "#6B7280",
    fontSize: 14,
    fontWeight: "700",
  },
  pageTitle: {
    color: "#111827",
    fontSize: 30,
    fontWeight: "900",
  },
  pressed: {
    opacity: 0.82,
  },
  section: {
    gap: 10,
  },
  settingLabel: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
  },
  settingLine: {
    backgroundColor: "#F7FAF2",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    gap: 4,
    padding: 12,
  },
  settingsStack: {
    gap: 10,
  },
  settingValue: {
    color: colors.mutedSoft,
    fontSize: 13,
    fontWeight: "800",
  },
  subtitle: {
    color: colors.accent,
    fontSize: 17,
    fontWeight: "800",
  },
  successText: {
    color: "#1E7A4C",
    fontSize: 14,
    fontWeight: "800",
  },
  title: {
    color: "#111827",
    fontSize: 28,
    fontWeight: "900",
  },
});
