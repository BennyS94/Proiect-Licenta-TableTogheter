import { Pressable, StyleSheet, Text, View } from "react-native";

type DaySelectorProps = {
  dayIndexes: number[];
  includeAverage?: boolean;
  onSelect: (value: number | "average") => void;
  selected: number | "average";
};

export function DaySelector({
  dayIndexes,
  includeAverage,
  onSelect,
  selected,
}: DaySelectorProps) {
  if (!includeAverage) {
    const selectedIndex = Math.max(
      0,
      dayIndexes.findIndex((dayIndex) => dayIndex === selected),
    );
    const selectedDay = dayIndexes[selectedIndex] ?? dayIndexes[0] ?? 1;
    const canCycle = dayIndexes.length > 1;

    function selectOffset(offset: number) {
      if (!dayIndexes.length) {
        return;
      }
      const nextIndex = (selectedIndex + offset + dayIndexes.length) % dayIndexes.length;
      onSelect(dayIndexes[nextIndex]);
    }

    return (
      <View style={styles.compactRow}>
        <Pressable
          accessibilityRole="button"
          disabled={!canCycle}
          onPress={() => selectOffset(-1)}
          style={({ pressed }) => [
            styles.arrowButton,
            pressed && canCycle ? styles.pressed : null,
            !canCycle ? styles.disabled : null,
          ]}
        >
          <Text style={styles.arrowText}>{"<"}</Text>
        </Pressable>
        <Text style={styles.compactLabel}>Day {selectedDay}</Text>
        <Pressable
          accessibilityRole="button"
          disabled={!canCycle}
          onPress={() => selectOffset(1)}
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

  const values: Array<number | "average"> = includeAverage
    ? [...dayIndexes, "average"]
    : dayIndexes;
  return (
    <View style={styles.row}>
      {values.map((value) => {
        const active = value === selected;
        return (
          <Pressable
            accessibilityRole="button"
            key={`${value}`}
            onPress={() => onSelect(value)}
            style={({ pressed }) => [
              styles.button,
              active ? styles.buttonActive : null,
              pressed ? styles.pressed : null,
            ]}
          >
            <Text style={[styles.label, active ? styles.labelActive : null]}>
              {value === "average" ? "Average" : `Day ${value}`}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  arrowButton: {
    alignItems: "center",
    backgroundColor: "#165D77",
    borderRadius: 8,
    height: 38,
    justifyContent: "center",
    width: 42,
  },
  arrowText: {
    color: "#FFFFFF",
    fontSize: 18,
    fontWeight: "900",
  },
  button: {
    borderColor: "#165D77",
    borderRadius: 8,
    borderWidth: 1,
    minHeight: 38,
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  buttonActive: {
    backgroundColor: "#165D77",
  },
  compactLabel: {
    color: "#111827",
    flex: 1,
    fontSize: 16,
    fontWeight: "900",
    textAlign: "center",
  },
  compactRow: {
    alignItems: "center",
    borderColor: "#D9D6CC",
    borderRadius: 8,
    borderWidth: 1,
    flexDirection: "row",
    gap: 12,
    padding: 10,
  },
  disabled: {
    opacity: 0.45,
  },
  label: {
    color: "#165D77",
    fontSize: 13,
    fontWeight: "800",
  },
  labelActive: {
    color: "#FFFFFF",
  },
  pressed: {
    opacity: 0.82,
  },
  row: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
});
