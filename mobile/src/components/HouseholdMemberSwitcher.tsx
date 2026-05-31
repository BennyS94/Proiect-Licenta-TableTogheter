import { Pressable, StyleSheet, Text, View } from "react-native";

import type { DemoMemberProfile } from "../types/api";

type HouseholdMemberSwitcherProps = {
  currentIndex: number;
  members: DemoMemberProfile[];
  onNext: () => void;
  onPrevious: () => void;
};

export function HouseholdMemberSwitcher({
  currentIndex,
  members,
  onNext,
  onPrevious,
}: HouseholdMemberSwitcherProps) {
  const currentMember = members[currentIndex] ?? null;
  const canCycle = members.length > 1;

  return (
    <View style={styles.container}>
      <Pressable
        accessibilityRole="button"
        disabled={!canCycle}
        onPress={onPrevious}
        style={({ pressed }) => [
          styles.arrowButton,
          pressed && canCycle ? styles.pressed : null,
          !canCycle ? styles.disabled : null,
        ]}
      >
        <Text style={styles.arrowText}>{"<"}</Text>
      </Pressable>

      <View style={styles.memberInfo}>
        <Text style={styles.memberName}>
          {currentMember ? memberName(currentMember) : "No member selected"}
        </Text>
        <Text style={styles.memberMeta}>
          {members.length ? `${currentIndex + 1} / ${members.length}` : "0 / 0"}
        </Text>
      </View>

      <Pressable
        accessibilityRole="button"
        disabled={!canCycle}
        onPress={onNext}
        style={({ pressed }) => [
          styles.arrowButton,
          pressed && canCycle ? styles.pressed : null,
          !canCycle ? styles.disabled : null,
        ]}
      >
        <Text style={styles.arrowText}>{">"}</Text>
      </Pressable>
    </View>
  );
}

function memberName(member: DemoMemberProfile): string {
  return String(
    member.display_name ?? member.profile_name ?? member.member_profile_id ?? member.member_id ?? "Member",
  );
}

const styles = StyleSheet.create({
  container: {
    alignItems: "center",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 12,
    padding: 12,
  },
  arrowButton: {
    alignItems: "center",
    backgroundColor: "#165D77",
    borderRadius: 8,
    height: 40,
    justifyContent: "center",
    width: 44,
  },
  arrowText: {
    color: "#FFFFFF",
    fontSize: 20,
    fontWeight: "900",
  },
  disabled: {
    opacity: 0.45,
  },
  memberInfo: {
    flex: 1,
    gap: 2,
  },
  memberMeta: {
    color: "#6B7280",
    fontSize: 13,
    fontWeight: "700",
  },
  memberName: {
    color: "#111827",
    fontSize: 17,
    fontWeight: "800",
  },
  pressed: {
    opacity: 0.82,
  },
});
