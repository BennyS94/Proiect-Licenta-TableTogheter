import { Pressable, StyleSheet, Text, View } from "react-native";

import { colors } from "../../theme/colors";

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
  const availableDays = new Set(dayIndexes);
  const values: Array<number | "average"> = includeAverage
    ? [1, 2, 3, 4, 5, "average"]
    : [1, 2, 3, 4, 5];
  return (
    <View style={styles.row}>
      {values.map((value) => {
        const active = value === selected;
        const disabled = typeof value === "number" && !availableDays.has(value);
        return (
          <Pressable
            accessibilityRole="button"
            disabled={disabled}
            key={`${value}`}
            onPress={() => onSelect(value)}
            style={({ pressed }) => [
              styles.button,
              active ? styles.buttonActive : null,
              disabled ? styles.buttonDisabled : null,
              pressed && !disabled ? styles.pressed : null,
            ]}
          >
            <Text
              style={[
                styles.label,
                active ? styles.labelActive : null,
                disabled ? styles.labelDisabled : null,
              ]}
            >
              {value === "average" ? "Average" : `Day ${value}`}
            </Text>
          </Pressable>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  button: {
    alignItems: "center",
    borderColor: colors.accent,
    borderRadius: 8,
    borderWidth: 1,
    flex: 1,
    justifyContent: "center",
    minHeight: 38,
    minWidth: 0,
    paddingHorizontal: 4,
    paddingVertical: 8,
  },
  buttonActive: {
    backgroundColor: colors.accent,
  },
  buttonDisabled: {
    backgroundColor: "#F3F4F6",
    borderColor: "#D1D5DB",
  },
  label: {
    color: colors.accent,
    fontSize: 12,
    fontWeight: "800",
    textAlign: "center",
  },
  labelActive: {
    color: "#FFFFFF",
  },
  labelDisabled: {
    color: "#9CA3AF",
  },
  pressed: {
    opacity: 0.82,
  },
  row: {
    flexDirection: "row",
    gap: 6,
    width: "100%",
  },
});
