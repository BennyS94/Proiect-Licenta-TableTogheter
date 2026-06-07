import { Pressable, StyleSheet, Text, View } from "react-native";

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
    <View style={styles.container}>
      <Pressable
        accessibilityLabel="Previous profile"
        accessibilityRole="button"
        disabled={!items.length}
        onPress={() => selectOffset(-1)}
        style={({ pressed }) => [styles.arrow, pressed ? styles.pressed : null]}
      >
        <Text style={styles.arrowText}>{"<"}</Text>
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
        <Text style={styles.arrowText}>{">"}</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  arrow: {
    alignItems: "center",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    height: 42,
    justifyContent: "center",
    width: 42,
  },
  arrowText: {
    color: "#165D77",
    fontSize: 20,
    fontWeight: "900",
  },
  center: {
    flex: 1,
    gap: 2,
  },
  container: {
    alignItems: "center",
    flexDirection: "row",
    gap: 10,
  },
  label: {
    color: "#111827",
    fontSize: 17,
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
