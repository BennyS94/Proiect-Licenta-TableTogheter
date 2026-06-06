import { Pressable, StyleSheet, Text, View } from "react-native";

import type { MemberProfileResponse } from "../types/api";

type ProfileCardProps = {
  deleteDisabled?: boolean;
  onPress: () => void;
  onDelete?: () => void;
  profile: MemberProfileResponse;
  selected: boolean;
};

export function ProfileCard({
  deleteDisabled,
  onDelete,
  onPress,
  profile,
  selected,
}: ProfileCardProps) {
  return (
    <View style={[styles.card, selected ? styles.cardSelected : null]}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ selected }}
        onPress={onPress}
        style={({ pressed }) => [styles.tapArea, pressed ? styles.cardPressed : null]}
      >
        <View style={styles.headerRow}>
          <Text style={styles.name}>{profile.display_name}</Text>
          <Text style={[styles.badge, selected ? styles.badgeSelected : null]}>
            {selected ? "Selected" : "Tap"}
          </Text>
        </View>
        <Text style={styles.meta}>
          {profile.age} years / {formatWeight(profile.weight_kg)} kg / {profile.activity_level}
        </Text>
        <Text style={styles.meta}>
          Goal: {profile.goal} / {profile.goal_speed}
        </Text>
      </Pressable>
      {onDelete ? (
        <Pressable
          accessibilityRole="button"
          disabled={deleteDisabled}
          onPress={onDelete}
          style={({ pressed }) => [
            styles.deleteButton,
            pressed && !deleteDisabled ? styles.cardPressed : null,
            deleteDisabled ? styles.deleteButtonDisabled : null,
          ]}
        >
          <Text style={styles.deleteButtonText}>
            {deleteDisabled ? "Removing" : "Remove"}
          </Text>
        </Pressable>
      ) : null}
    </View>
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

const styles = StyleSheet.create({
  badge: {
    color: "#6B7280",
    fontSize: 13,
    fontWeight: "800",
  },
  badgeSelected: {
    color: "#165D77",
  },
  card: {
    backgroundColor: "#FFFFFF",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    gap: 6,
    padding: 14,
  },
  cardPressed: {
    opacity: 0.82,
  },
  cardSelected: {
    backgroundColor: "#EEF7F8",
    borderColor: "#165D77",
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
  name: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 16,
    fontWeight: "800",
  },
  deleteButton: {
    alignItems: "center",
    alignSelf: "flex-start",
    borderColor: "#B42318",
    borderRadius: 8,
    borderWidth: 1,
    minHeight: 36,
    justifyContent: "center",
    paddingHorizontal: 12,
  },
  deleteButtonDisabled: {
    opacity: 0.55,
  },
  deleteButtonText: {
    color: "#B42318",
    fontSize: 13,
    fontWeight: "800",
  },
  tapArea: {
    gap: 6,
  },
});
