import { Pressable, StyleSheet, Text, View } from "react-native";

import { ChevronDownIcon } from "../icons/ChevronDownIcon";
import { colors } from "../../theme/colors";

export type ProfileSelectorItem = {
  id: string;
  label: string;
  meta?: string;
};

type ProfileSelectorProps = {
  items: ProfileSelectorItem[];
  onSelect: (id: string) => void;
  selectedId?: string;
};

export function ProfileSelector({ items, onSelect, selectedId }: ProfileSelectorProps) {
  const selectedIndex = Math.max(
    0,
    items.findIndex((item) => item.id === selectedId),
  );
  const selected = items[selectedIndex] ?? null;

  function selectOffset(offset: number) {
    if (!items.length) {
      return;
    }
    const nextIndex = (selectedIndex + offset + items.length) % items.length;
    onSelect(items[nextIndex].id);
  }

  return (
    <View style={styles.selector}>
      <Pressable
        accessibilityLabel="Previous profile"
        accessibilityRole="button"
        disabled={!items.length}
        onPress={() => selectOffset(-1)}
        style={({ pressed }) => [styles.arrow, pressed ? styles.pressed : null]}
      >
        <View style={styles.chevronLeft}>
          <ChevronDownIcon color={colors.accent} size={18} />
        </View>
      </Pressable>

      <View style={styles.center}>
        <Text style={styles.label}>{selected?.label ?? "No profile selected"}</Text>
        {selected?.meta ? <Text style={styles.meta}>{selected.meta}</Text> : null}
      </View>

      <Pressable
        accessibilityLabel="Next profile"
        accessibilityRole="button"
        disabled={!items.length}
        onPress={() => selectOffset(1)}
        style={({ pressed }) => [styles.arrow, pressed ? styles.pressed : null]}
      >
        <View style={styles.chevronRight}>
          <ChevronDownIcon color={colors.accent} size={18} />
        </View>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  arrow: {
    alignItems: "center",
    justifyContent: "center",
    minHeight: 42,
    width: 42,
  },
  chevronLeft: {
    transform: [{ rotate: "90deg" }],
  },
  chevronRight: {
    transform: [{ rotate: "-90deg" }],
  },
  center: {
    alignItems: "center",
    flex: 1,
    gap: 2,
    justifyContent: "center",
    minHeight: 44,
    minWidth: 0,
  },
  selector: {
    alignItems: "center",
    backgroundColor: "#F8FBF3",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    minHeight: 46,
  },
  label: {
    color: "#111827",
    fontSize: 15,
    fontWeight: "800",
    textAlign: "center",
  },
  meta: {
    color: "#6B7280",
    fontSize: 13,
    fontWeight: "700",
    textAlign: "center",
  },
  pressed: {
    opacity: 0.82,
  },
});
