import { StyleSheet, Text, View } from "react-native";

type MacroBarProps = {
  actual?: number | null;
  color: string;
  label: string;
  target?: number | null;
  unit?: string;
};

export function MacroBar({ actual, color, label, target, unit = "" }: MacroBarProps) {
  const safeActual = isFiniteNumber(actual) ? actual : null;
  const safeTarget = isFiniteNumber(target) && target > 0 ? target : null;
  const percent = safeActual !== null && safeTarget !== null
    ? Math.min(100, Math.max(0, (safeActual / safeTarget) * 100))
    : safeActual !== null
      ? 100
      : 0;
  const valueText =
    safeActual === null
      ? "-"
      : safeTarget !== null
        ? `${formatNumber(safeActual)}${unit} / ${formatNumber(safeTarget)}${unit}`
        : `${formatNumber(safeActual)}${unit}`;

  return (
    <View style={styles.container}>
      <View style={styles.row}>
        <Text style={styles.label}>{label}</Text>
        <Text style={styles.value}>{valueText}</Text>
      </View>
      <View style={styles.track}>
        <View style={[styles.fill, { backgroundColor: color, width: `${percent}%` }]} />
      </View>
    </View>
  );
}

function formatNumber(value: number): string {
  return String(Math.round(value));
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

const styles = StyleSheet.create({
  container: {
    gap: 6,
  },
  fill: {
    borderRadius: 999,
    height: "100%",
  },
  label: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "700",
  },
  row: {
    flexDirection: "row",
    gap: 12,
    justifyContent: "space-between",
  },
  track: {
    backgroundColor: "#F0EFE8",
    borderRadius: 999,
    height: 10,
    overflow: "hidden",
  },
  value: {
    color: "#111827",
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "800",
    textAlign: "right",
  },
});
