import { StyleSheet, Text, View } from "react-native";

type MacroDonutPlaceholderProps = {
  carbsPercent: number;
  fatPercent: number;
  proteinPercent: number;
};

export function MacroDonutPlaceholder({
  carbsPercent,
  fatPercent,
  proteinPercent,
}: MacroDonutPlaceholderProps) {
  return (
    <View style={styles.wrap}>
      <View style={styles.ring}>
        <View style={[styles.segment, styles.protein]} />
        <View style={[styles.segment, styles.carbs]} />
        <View style={[styles.segment, styles.fat]} />
        <View style={styles.center}>
          <Text style={styles.centerText}>Macros</Text>
        </View>
      </View>
      <View style={styles.legend}>
        <LegendItem color="#D64B4B" label={`Protein ${formatPercent(proteinPercent)}`} />
        <LegendItem color="#E58A2F" label={`Carbs ${formatPercent(carbsPercent)}`} />
        <LegendItem color="#E2C84C" label={`Fats ${formatPercent(fatPercent)}`} />
      </View>
    </View>
  );
}

function LegendItem({ color, label }: { color: string; label: string }) {
  return (
    <View style={styles.legendItem}>
      <View style={[styles.swatch, { backgroundColor: color }]} />
      <Text style={styles.legendText}>{label}</Text>
    </View>
  );
}

function formatPercent(value: number): string {
  if (!Number.isFinite(value)) {
    return "-";
  }
  return `${Math.round(value)}%`;
}

const styles = StyleSheet.create({
  carbs: {
    backgroundColor: "#E58A2F",
    height: 82,
    right: 8,
    top: 30,
    width: 26,
  },
  center: {
    alignItems: "center",
    backgroundColor: "#FFFFFF",
    borderRadius: 48,
    height: 78,
    justifyContent: "center",
    position: "absolute",
    width: 78,
  },
  centerText: {
    color: "#111827",
    fontSize: 13,
    fontWeight: "900",
  },
  fat: {
    backgroundColor: "#E2C84C",
    bottom: 10,
    height: 36,
    left: 24,
    width: 90,
  },
  legend: {
    flex: 1,
    gap: 8,
  },
  legendItem: {
    alignItems: "center",
    flexDirection: "row",
    gap: 8,
  },
  legendText: {
    color: "#4B5563",
    fontSize: 14,
    fontWeight: "700",
  },
  protein: {
    backgroundColor: "#D64B4B",
    height: 88,
    left: 10,
    top: 10,
    width: 58,
  },
  ring: {
    alignItems: "center",
    backgroundColor: "#F0EFE8",
    borderRadius: 70,
    height: 140,
    justifyContent: "center",
    overflow: "hidden",
    width: 140,
  },
  segment: {
    position: "absolute",
  },
  swatch: {
    borderRadius: 4,
    height: 12,
    width: 12,
  },
  wrap: {
    alignItems: "center",
    flexDirection: "row",
    gap: 18,
  },
});
