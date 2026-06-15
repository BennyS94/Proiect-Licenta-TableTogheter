import { Pressable, StyleSheet, Text, View } from "react-native";

import type { MemberProfileResponse } from "../types/api";

type ProfileCardProps = {
  onPress: () => void;
  profile: MemberProfileResponse;
  selected: boolean;
};

export function ProfileCard({
  onPress,
  profile,
  selected,
}: ProfileCardProps) {
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityState={{ selected }}
      onPress={onPress}
      style={({ pressed }) => [
        styles.card,
        selected ? styles.cardSelected : null,
        pressed ? styles.cardPressed : null,
      ]}
    >
      <View style={styles.cardBody}>
        <View style={styles.headerRow}>
          <Text numberOfLines={1} style={styles.name}>
            {profile.display_name}
          </Text>
          <Text style={styles.chevron}>{">"}</Text>
        </View>
        <Text numberOfLines={1} style={styles.meta}>
          {profile.age} years | {formatWeight(profile.weight_kg)} kg |{" "}
          {formatActivityLevel(profile.activity_level)}
        </Text>
        <Text numberOfLines={1} style={styles.goalText}>
          {formatGoalSummary(profile.goal, profile.goal_speed)}
        </Text>
      </View>
    </Pressable>
  );
}

function formatWeight(value: number): string {
  if (!Number.isFinite(value)) {
    return "?";
  }
  return value.toLocaleString("en-US", {
    maximumFractionDigits: 1,
    minimumFractionDigits: 0,
  });
}

function formatActivityLevel(value: string): string {
  const labels: Record<string, string> = {
    sedentary: "Sedentary",
    lightly_active: "Lightly active",
    moderately_active: "Moderately active",
    very_active: "Very active",
  };
  return labels[value] ?? titleize(value);
}

function formatGoalSummary(goal: string, speed: string): string {
  const normalizedGoal = goal.trim().toLowerCase();
  if (normalizedGoal === "maintain") {
    return "Maintain";
  }
  const goalLabel =
    normalizedGoal === "gain"
      ? "Muscle gain"
      : normalizedGoal === "lose"
      ? "Weight loss"
      : titleize(normalizedGoal);
  const speedLabel = speed.trim().toLowerCase()
    ? titleize(speed)
    : "Normal";
  return `${goalLabel} | ${speedLabel} pace`;
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    padding: 14,
  },
  cardBody: {
    gap: 6,
  },
  cardPressed: {
    opacity: 0.82,
  },
  cardSelected: {
    backgroundColor: "#F7FAF2",
    borderColor: "#74B72E",
  },
  chevron: {
    color: "#74B72E",
    fontSize: 20,
    fontWeight: "900",
  },
  headerRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  meta: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "600",
  },
  goalText: {
    color: "#4F8F1F",
    fontSize: 14,
    fontWeight: "800",
  },
  name: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 16,
    fontWeight: "800",
  },
});
