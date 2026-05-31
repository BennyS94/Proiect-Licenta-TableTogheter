import { Pressable, StyleSheet, Text, View } from "react-native";

import type { DemoMemberProfile } from "../types/api";

type MemberCardProps = {
  member: DemoMemberProfile;
  selected: boolean;
  onPress: () => void;
};

export function MemberCard({ member, selected, onPress }: MemberCardProps) {
  const displayName = String(member.display_name ?? member.profile_name ?? "Member");
  const memberGoal = String(member.goal ?? "goal unknown");

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
      <View style={styles.headerRow}>
        <Text style={styles.name}>{displayName}</Text>
        <Text style={[styles.badge, selected ? styles.badgeSelected : null]}>
          {selected ? "Selected" : "Tap"}
        </Text>
      </View>
      <Text style={styles.meta}>
        {member.age ?? "?"} years · {String(member.sex ?? "sex unknown")}
      </Text>
      <Text style={styles.meta}>Goal: {memberGoal}</Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: 1,
    borderColor: "#D9D6CC",
    borderRadius: 8,
    backgroundColor: "#FFFFFF",
    padding: 14,
    gap: 6,
  },
  cardSelected: {
    borderColor: "#165D77",
    backgroundColor: "#EEF7F8",
  },
  cardPressed: {
    opacity: 0.82,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  name: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 16,
    fontWeight: "800",
  },
  badge: {
    color: "#6B7280",
    fontSize: 13,
    fontWeight: "800",
  },
  badgeSelected: {
    color: "#165D77",
  },
  meta: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "600",
  },
});
