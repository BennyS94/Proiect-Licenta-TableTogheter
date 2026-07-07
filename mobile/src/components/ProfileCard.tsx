import { Pressable, StyleSheet, Text, View } from "react-native";

import { colors } from "../theme/colors";
import type { MemberProfileResponse } from "../types/api";

type ProfileCardProps = {
  deleteDisabled?: boolean;
  onDelete: () => void;
  onEdit: () => void;
  profile: MemberProfileResponse;
  selected: boolean;
};

export function ProfileCard({
  deleteDisabled,
  onDelete,
  onEdit,
  profile,
  selected,
}: ProfileCardProps) {
  const dietaryBadges = getDietaryBadges(profile);

  return (
    <View style={[styles.card, selected ? styles.cardSelected : null]}>
      <View style={styles.cardBody}>
        <View style={styles.headerRow}>
          <Text numberOfLines={1} style={styles.name}>
            {profile.display_name}
          </Text>
          <Text numberOfLines={1} style={styles.goalBadge}>
            {formatGoalLabel(profile.goal)}
          </Text>
        </View>
        <Text numberOfLines={1} style={styles.meta}>
          {profile.age} years | {formatWeight(profile.weight_kg)} kg |{" "}
          {formatActivityLevel(profile.activity_level)}
        </Text>
        {dietaryBadges.length ? (
          <View style={styles.badgeRow}>
            {dietaryBadges.map((badge) => (
              <Text key={badge} numberOfLines={1} style={styles.dietaryBadge}>
                {badge}
              </Text>
            ))}
          </View>
        ) : null}
      </View>
      <View style={styles.actionRow}>
        <Pressable
          accessibilityRole="button"
          onPress={onEdit}
          style={({ pressed }) => [
            styles.actionButton,
            pressed ? styles.cardPressed : null,
          ]}
        >
          <Text style={styles.actionButtonText}>Edit</Text>
        </Pressable>
        <Pressable
          accessibilityRole="button"
          disabled={deleteDisabled}
          onPress={onDelete}
          style={({ pressed }) => [
            styles.actionButton,
            styles.deleteButton,
            deleteDisabled ? styles.disabled : null,
            pressed && !deleteDisabled ? styles.cardPressed : null,
          ]}
        >
          <Text style={styles.deleteButtonText}>
            {deleteDisabled ? "Deleting" : "Delete"}
          </Text>
        </Pressable>
      </View>
    </View>
  );
}

function getDietaryBadges(profile: MemberProfileResponse): string[] {
  const dietary = profile.dietary_preferences ?? {};
  const badges: string[] = [];
  if (dietary.vegan) {
    badges.push("Vegan");
  } else if (dietary.vegetarian) {
    badges.push("Vegetarian");
  }
  if (dietary.gluten_free) {
    badges.push("Gluten-free");
  }
  return badges;
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

function formatGoalLabel(goal: string): string {
  const normalizedGoal = goal.trim().toLowerCase();
  if (normalizedGoal === "maintain") {
    return "Maintain";
  }
  if (normalizedGoal === "balanced") {
    return "Balanced eating";
  }
  const goalLabel =
    normalizedGoal === "gain"
      ? "Muscle gain"
      : normalizedGoal === "lose"
      ? "Weight loss"
      : titleize(normalizedGoal);
  return goalLabel;
}

function titleize(value: string): string {
  return value
    .replace(/_/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const styles = StyleSheet.create({
  actionButton: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 38,
  },
  actionButtonText: {
    color: colors.accent,
    fontSize: 14,
    fontWeight: "900",
  },
  actionRow: {
    flexDirection: "row",
    gap: 8,
    marginTop: 12,
  },
  card: {
    backgroundColor: "#FFFFFF",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    padding: 14,
  },
  cardBody: {
    gap: 6,
  },
  badgeRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  cardPressed: {
    opacity: 0.82,
  },
  cardSelected: {
    backgroundColor: "#FFFFFF",
    borderColor: "#DDEAD3",
  },
  deleteButton: {
    borderColor: colors.danger,
  },
  deleteButtonText: {
    color: colors.danger,
    fontSize: 14,
    fontWeight: "900",
  },
  disabled: {
    opacity: 0.55,
  },
  dietaryBadge: {
    backgroundColor: "#F7FAF1",
    borderColor: "#DDEAD3",
    borderRadius: 999,
    borderWidth: 1,
    color: colors.accentDark,
    fontSize: 12,
    fontWeight: "800",
    lineHeight: 15,
    paddingHorizontal: 8,
    paddingVertical: 3,
  },
  headerRow: {
    alignItems: "center",
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  goalBadge: {
    backgroundColor: "#F1F8EA",
    borderColor: "#DDEFCF",
    borderRadius: 999,
    borderWidth: 1,
    color: colors.accentDark,
    flexShrink: 0,
    fontSize: 12,
    fontWeight: "800",
    lineHeight: 15,
    maxWidth: "44%",
    overflow: "hidden",
    paddingHorizontal: 8,
    paddingVertical: 3,
    textAlign: "center",
  },
  meta: {
    color: colors.muted,
    fontSize: 14,
    fontWeight: "600",
  },
  name: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 16,
    fontWeight: "800",
  },
});
